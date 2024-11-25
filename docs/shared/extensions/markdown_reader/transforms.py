"""Perform transformations required by markdown reader."""
from docutils import nodes
from sphinx import addnodes
from docutils.transforms.parts import Contents
from docutils.transforms.references import ExternalTargets
from markdown_utils import make_target_name
from sphinx.transforms import DoctreeReadEvent
from sphinx.transforms import SphinxTransform
from sphinx.transforms.references import SphinxDomains


class RewriteReferenceHashesTransform(SphinxTransform):
    """Instantiates the rewrite reference hashes transform class.

    Should be invoked before Domains process.

    """
    #: Set the default priority.
    default_priority = min(SphinxDomains.default_priority,
                           DoctreeReadEvent.default_priority,
                           Contents.default_priority) - 2

    def apply(self, **kwargs):
        """Apply the required transformations.

        .. todo:: This is way too complicated.

        :param self: Current instance.
        :param **kwargs: Keyword arguments.
        """
        target_starts_with = make_target_name(self.document, '')
        len_target_starts_with = len(target_starts_with)

        replace = []
        for name in self.document.nametypes:
            if not self.document.nametypes[name]:
                continue
            labelid = self.document.nameids[name]
            if labelid is None or not labelid.startswith(target_starts_with):
                continue
            replace.append(labelid)

        for name in replace:
            prev_id = self.document.nameids[name]
            node = self.document.ids[prev_id]
            # indirect hyperlink targets
            if node.tagname == 'target' and 'refid' in node:
                node = self.document.ids.get(node['refid'])
                names = node['names']
                prev_name = names[0]
                if prev_name.startswith(target_starts_with):
                    new_name = prev_name[len_target_starts_with:]
                    self.document.nameids[new_name] = (
                        self.document.nameids[prev_name])
                    self.document.nametypes[new_name] = (
                        self.document.nametypes[prev_name])
                    del self.document.nameids[prev_name]
                    del self.document.nametypes[prev_name]
            else:
                new_id = prev_id
                if prev_id.startswith(target_starts_with):
                    new_id = prev_id[len_target_starts_with:]
                    node = self.document.ids[prev_id]
                    node['ids'] = [new_id if id ==
                                   prev_id else id for id in node['ids']]
                    self.document.ids[new_id] = node
                    del self.document.ids[prev_id]

                self.document.nameids[name] = new_id


class RewriteTargetRefIdsTransform(SphinxTransform):
    """Instantiates the RewriteTargetRefIdsTransform class."""
    #: Set the default priority.
    default_priority = RewriteReferenceHashesTransform.default_priority - 1

    def apply(self, **kwargs):
        """Apply the required transformations

        :param self: Current instance.
        :param **kwargs: Keyword arguments.
        """
        target_starts_with = make_target_name(self.document, '')
        len_target_starts_with = len(target_starts_with)

        for target in self.document.traverse(nodes.target):
            if 'refid' in target and target['refid'].startswith(
                    target_starts_with):
                target['refid'] = target['refid'][len_target_starts_with:]


class RewriteSectionsIdsTransform(SphinxTransform):
    """Instantiates the RewriteSectionIdsTransform class."""
    #: Set the default priority.
    default_priority = min(DoctreeReadEvent.default_priority,
                           Contents.default_priority) - 1

    def apply(self, **kwargs):
        """Updates the IDs of sections to proper IDs.

        :param self: Current instance.
        :param **kwargs: Keyword arguments.
        """
        for section in self.document.traverse(nodes.section):
            title = section[0]
            prev_id = section['ids'][0]
            new_id = nodes.make_id(title.astext().strip())

            if (
                new_id != '' and
                prev_id != new_id and
                new_id not in section['ids']
            ):
                section['ids'].insert(0, new_id)


class ImageExternalTargets(SphinxTransform):
    """Instantiates the ImageExxternalTargets class."""
    #: Set the default priority.
    default_priority = ExternalTargets.default_priority + 1

    def apply(self, **kwargs):
        """Sets 'uri' attribute for images.

        Written as ![image title][image_target] [image_target]: <image_uri>

        :param self: Current instance.
        :param **kwargs: Keyword arguments.
        """
        for img in self.document.traverse(nodes.image):
            if 'refuri' in img and 'uri' not in img:
                img['uri'] = img['refuri']
                del img['refuri']


class ImageTargetsCaptions(SphinxTransform):
    """Instantiates the ImageTargetsCaptions transform class."""
    #: Set the default priority.
    default_priority = ExternalTargets.default_priority - 1

    def apply(self, **kwargs):
        """Adds figure titles for images whose targets had titles

        e.g. ![img alt text][target_name] [target_name]: <uri> "title"

        .. todo:: This is too complex.

        :param self: Current instance.
        :param **kwargs: Keyword arguments.
        """
        for target in self.document.traverse(nodes.target):
            if 'figure_title' in target:
                for name in target['names']:
                    reflist = self.document.refnames.get(name, [])
                    for ref in reflist:
                        if isinstance(ref, nodes.image):
                            ref.children.append(
                                nodes.caption(
                                    target['figure_title'],
                                    target['figure_title'])
                                )


class DocinfoTransform(SphinxTransform):
    """Instantiates the DocinfoTransform class."""
    #: Set the default priority.
    default_priority = DoctreeReadEvent.default_priority - 1

    def apply(self, **kwargs):
        """Apply the required transformations.

        .. todo: This is too complex.

        :param self: Current instance.
        :param **kwargs: Keyword arguments.
        """
        docinfo_meta = []
        for meta in self.document.traverse(addnodes.meta):
            if (meta['name'] in ['orphan', 'tocdepth']):
                docinfo_meta.append((meta['name'], meta.children))
                meta.parent.remove(meta)
        if docinfo_meta:
            docinfo_index = self.document.first_child_matching_class(
                nodes.docinfo)
            if docinfo_index is None:
                docinfo = nodes.docinfo()
                self.document.insert(0, docinfo)
            else:
                docinfo = self.document[docinfo_index]
            for (key, value) in docinfo_meta:
                field = nodes.field()
                field.append(nodes.field_name(key, key))
                field.append(nodes.field_body('', *value))
                docinfo.append(field)
