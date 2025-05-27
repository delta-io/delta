/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.util;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.function.Consumer;

/** Utility class for iterating over schema structures and modifying them. */
public class SchemaIterable implements Iterable<SchemaIterable.SchemaElement> {
  private StructType schema;

  /** Construct a new Iterable for the schema. */
  public SchemaIterable(StructType schema) {
    this.schema = schema;
  }

  /**
   * Gets the latest schema (either the initial schema or the one set after a mutable iterator is
   * fully consumed.
   */
  public StructType getSchema() {
    return schema;
  }

  @Override
  public Iterator<SchemaElement> iterator() {
    Iterator<MutableSchemaElement> iterator = new SchemaIterator(schema, structType -> {});
    return new Iterator<SchemaElement>() {

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public SchemaElement next() {
        return iterator.next();
      }
    };
  }

  /**
   * Returns a new iterator that can be used to iterate and update elements in the schema. The
   * current schema on this iterable is updated once the returned iterator is fully consumed.
   *
   * <p>Consuming multiple iterators across different threads concurrently is not thread safe.
   */
  public Iterator<MutableSchemaElement> newMutableIterator() {
    return new SchemaIterator(schema, this::setSchema);
  }

  private void setSchema(StructType newSchema) {
    this.schema = newSchema;
  }

  /**
   * Iterator that performs a depth-first traversal of a schema structure using a zipper pattern.
   * Each call to next() returns the next zipper in the traversal sequence.
   */
  private static class SchemaIterator implements Iterator<MutableSchemaElement> {
    private final Consumer<StructType> finalizedSchemaConsumer;
    private SchemaZipper nextZipper;
    private boolean finishedVisitingCurrent = false;

    SchemaIterator(StructType schema, Consumer<StructType> finalizedSchemaConsumer) {
      this.nextZipper = SchemaZipper.createZipper(schema);
      this.finalizedSchemaConsumer = finalizedSchemaConsumer;
      // Special case if struct is empty no force no iteration.
      this.finishedVisitingCurrent = schema.fields().isEmpty();
    }

    @Override
    public boolean hasNext() {
      boolean nextAvailable =
          nextZipper != null
              && (!finishedVisitingCurrent // Implies there are children.
                  // Without children there must be siblings or parents left to visit to have a next
                  // value.
                  || nextZipper.hasMoreSiblings()
                  || nextZipper.hasParents());
      if (!nextAvailable && nextZipper != null) {
        finalizedSchemaConsumer.accept((StructType) nextZipper.extractDataTypeFromFields());
        nextZipper = null;
      }
      return nextAvailable;
    }

    @Override
    public MutableSchemaElement next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      advanceNext();
      return nextZipper;
    }

    private void advanceNext() {
      while (nextZipper != null) {
        if (!finishedVisitingCurrent) {
          if (nextZipper.hasChildren()) {
            // Try to go deeper if we haven't visited this node yet
            while (nextZipper.hasChildren()) {
              nextZipper = nextZipper.childrenZipper();
            }
          }
          // At a leaf node, so by definition it is finished visiting.
          finishedVisitingCurrent = true;
          return;
        }

        // Try moving to sibling if there is no need to go down further.
        if (nextZipper.hasMoreSiblings()) {
          nextZipper = nextZipper.moveToSibling();
          if (nextZipper.hasChildren()) {
            // Force visiting children first.
            finishedVisitingCurrent = false;
            continue;
          }
          finishedVisitingCurrent = true;
          return;
        }

        // Last remaining direction with no children and no
        // siblings is to pop back up and note that visiting
        // has finished on the current value.
        nextZipper = nextZipper.moveToParent();
        finishedVisitingCurrent = true;
        return;
      }
    }
  }

  /**
   * Interface for representing a schema element as part of a traversal.
   *
   * <p>This object should always be treated ephemeral and not be referenced once {@code next()} is
   * called on the iterator.
   */
  public interface SchemaElement {
    /** Get the current field. */
    StructField getField();

    /** Returns the path to the node via user facing names. */
    String getNamePath();

    /**
     * Returns the nearest ancestor that is a member of a StructType (could be the current element).
     *
     * <p>Maps Keys and Values and Array elements are skipped over when finding the nearest
     * ancestor.
     */
    StructField getNearestStructFieldAncestor();

    /**
     * Returns the path to this node from the nearest ancestor that is a member of a StructType.
     *
     * <p>Prefix is prepend to any path with an added "."
     *
     * <p>If this element is a StructField returns prefix
     *
     * <p>Otherwise the grammar of the returned field is: {@code
     * [<prefix>.]((key|value|element).)*(key|value|element)}
     */
    String getPathFromNearestStructFieldAncestor(String prefix);
  }
  /**
   * Interface for manipulating Schema elements.
   *
   * <p>This object should always be treated ephemeral and not be referenced once {@code next()} is
   * called on the iterator.
   */
  public interface MutableSchemaElement extends SchemaElement {
    /** Replace the current targeted field with a new field. */
    void updateField(StructField structField);

    /** Replace the metadata on the nearest struct ancestor with new metadata. */
    void setMetadataOnNearestStructFieldAncestor(FieldMetadata metadata);
  }

  /**
   * SchemaZipper implements an adaptation of the functional zipper pattern for manipulating schema
   * structures.
   *
   * <p>As a high-level summary, keeps state of the path used to get a certain element as it moves
   * through Schema elements. As it moves back up, it reconstructs the schema data types as
   * necessary.
   *
   * <p>N.B. For clients using this class only one instance of a Zipper should be kept around since
   * the internal state is mutable.
   */
  private abstract static class SchemaZipper implements MutableSchemaElement {
    // Path to the current zipper. Note parents is shared between all elements
    // on the path.
    private final List<SchemaZipper> parents;

    abstract DataType constructType();

    protected List<StructField> fields;
    // Current focus element in fields.
    private int index = 0;
    private boolean modified = false;

    private SchemaZipper(List<SchemaZipper> parents, List<StructField> fields) {
      this.parents = parents;
      this.fields = fields;
    }

    /** Returns if the zipper has any children can be traversed. */
    public boolean hasChildren() {
      boolean isStructType = currentField().getDataType() instanceof StructType;
      // TODO(#4571): this concept should be centralized.
      boolean isNested =
          isStructType
              || currentField().getDataType() instanceof ArrayType
              || currentField().getDataType() instanceof MapType;
      boolean isEmptyStruct =
          isStructType && ((StructType) currentField().getDataType()).fields().isEmpty();
      return isNested && !isEmptyStruct;
    }

    static SchemaZipper createZipper(StructType schema) {
      return createZipper(/*parents=*/ new ArrayList<>(), schema);
    }

    private static SchemaZipper createZipper(List<SchemaZipper> parents, DataType type) {
      if (type instanceof ArrayType) {
        ArrayType arrayType = (ArrayType) type;
        return new ArraySchemaZipper(parents, arrayType);
      } else if (type instanceof MapType) {
        MapType mapType = (MapType) type;
        return new MapSchemaZipper(parents, mapType);
      } else if (type instanceof StructType) {
        StructType structType = (StructType) type;
        return new StructSchemaZipper(parents, structType);
      } else {
        throw new KernelException("Unsupported data type: " + type);
      }
    }

    private static SchemaZipper createZipper(List<SchemaZipper> parents, StructField field) {
      return createZipper(parents, field.getDataType());
    }

    /** Returns a zipper pointing the left-most child field of this zipper. */
    public SchemaZipper childrenZipper() {
      if (!hasChildren()) {
        return null;
      }
      parents.add(this);
      return createZipper(parents, fields.get(index));
    }

    /**
     * Returns a zipper pointing to the next sibling of this zipper. (moving right across zippers).
     */
    public SchemaZipper moveToSibling() {
      if (!hasMoreSiblings()) {
        return null;
      }
      index++;
      return this;
    }

    public boolean hasMoreSiblings() {
      return index < fields.size() - 1;
    }

    @Override
    public StructField getField() {
      return currentField();
    }

    private SchemaZipper getParent() {
      if (parents.isEmpty()) {
        return null;
      }
      return parents.get(parents.size() - 1);
    }

    /**
     * Returns a new zipper pointing to the parent of this zipper.
     *
     * <p>If the zipper has any modifications they are propagated up to the parent.
     */
    public SchemaZipper moveToParent() {
      SchemaZipper parent = getParent();
      if (!parents.isEmpty()) {
        parents.remove(parents.size() - 1);
      } else {
        return null;
      }
      if (modified) {
        // Propagate changes to parent.
        StructField currentParentField = parent.currentField();
        StructField newParentField = currentParentField.withDataType(extractDataTypeFromFields());
        parent.updateField(newParentField);
      }
      return parent;
    }

    public DataType extractDataTypeFromFields() {
      return constructType();
    }

    public StructField currentField() {
      return fields.get(index);
    }

    @Override
    public void updateField(StructField structField) {
      try {
        fields.set(index, structField);
      } catch (UnsupportedOperationException e) {
        // Field might be immutable, copy and set if this is the case.
        fields = new ArrayList<>(fields);
        fields.set(index, structField);
      }
      modified = true;
    }

    @Override
    public String getNamePath() {
      // TODO: Escape names that have '.' in them.
      int size =
          parents.stream().mapToInt(p -> p.currentField().getName().length()).sum()
              + currentField().getName().length();
      StringBuilder sb = new StringBuilder(size + parents.size());
      for (SchemaZipper parent : parents) {
        sb.append(parent.currentField().getName());
        sb.append(".");
      }
      sb.append(currentField().getName());
      return sb.toString();
    }

    @Override
    public StructField getNearestStructFieldAncestor() {
      if (parents.isEmpty() || this instanceof StructSchemaZipper) {
        return currentField();
      }
      ListIterator<SchemaZipper> iterator = findNearestStructFieldAncestor();
      return iterator.next().currentField();
    }

    /** Returns an iterator to a zipper that has a focus on a StructType child StructField. */
    private ListIterator<SchemaZipper> findNearestStructFieldAncestor() {
      ListIterator<SchemaZipper> iterator = parents.listIterator(parents.size());
      while (iterator.hasPrevious()) {
        SchemaZipper parent = iterator.previous();
        if (parent instanceof StructSchemaZipper) {
          return iterator;
        }
      }
      throw new IllegalArgumentException("no top level parent struct field, this shouldn't happen");
    }

    @Override
    public void setMetadataOnNearestStructFieldAncestor(FieldMetadata metadata) {
      if (parents.isEmpty() || this instanceof StructSchemaZipper) {
        updateField(currentField().withNewMetadata(metadata));
        return;
      }
      ListIterator<SchemaZipper> iterator = findNearestStructFieldAncestor();
      SchemaZipper parent = iterator.next();
      parent.updateField(parent.currentField().withNewMetadata(metadata));
    }

    @Override
    public String getPathFromNearestStructFieldAncestor(String prefix) {
      if (parents.isEmpty() || this instanceof StructSchemaZipper) {
        return prefix;
      }
      ListIterator<SchemaZipper> iterator = parents.listIterator(parents.size());
      int pathSize = prefix.length() + currentField().getName().length();
      while (iterator.hasPrevious()) {
        SchemaZipper parent = iterator.previous();
        if (parent instanceof StructSchemaZipper) {
          break;
        }
        pathSize += parent.currentField().getName().length();
      }
      StringBuilder sb =
          new StringBuilder(
              pathSize + (prefix.isEmpty() ? 0 : 1 + (parents.size()) - iterator.nextIndex()));
      if (!prefix.isEmpty()) {
        sb.append(prefix);
        sb.append(".");
      }
      iterator.next();
      while (iterator.hasNext()) {
        sb.append(iterator.next().currentField().getName());
        sb.append(".");
      }
      sb.append(currentField().getName());
      return sb.toString();
    }

    public boolean hasParents() {
      return !parents.isEmpty();
    }
  }

  private static class ArraySchemaZipper extends SchemaZipper {
    ArraySchemaZipper(List<SchemaZipper> parents, ArrayType arrayType) {
      super(parents, Collections.singletonList(arrayType.getElementField()));
    }

    @Override
    DataType constructType() {
      return new ArrayType(fields.get(0));
    }
  }

  private static class MapSchemaZipper extends SchemaZipper {
    MapSchemaZipper(List<SchemaZipper> parents, MapType mapType) {
      super(parents, Arrays.asList(mapType.getKeyField(), mapType.getValueField()));
    }

    @Override
    DataType constructType() {
      return new MapType(fields.get(0), fields.get(1));
    }
  }

  private static class StructSchemaZipper extends SchemaZipper {
    StructSchemaZipper(List<SchemaZipper> parents, StructType structType) {
      super(parents, structType.fields());
    }

    @Override
    DataType constructType() {
      return new StructType(fields);
    }
  }
}
