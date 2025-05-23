# Metadata

The module teaches [Sphinx](http://www.sphinx-doc.org/) to evaluate substitution references inside document metadata.

Tested with Sphinx 1.8.5.

## Usage

### conf.py

```python
extensions = ['metadata']
```

### Later in docs

Restructured text form:

```restructuredtext
.. meta::
    :title: Title text with |DEFINITION| included
```

Markdown form:

```markdown
    ---
    title: Title text with <DEFINITION> included
    ---
```
