"""Special rules for the markdown domain."""
from os import path

from docutils import nodes, utils
from markdown_utils import target_file_sep
from sphinx.domains import Domain
from sphinx.roles import XRefRole
from sphinx.util.nodes import set_role_source_info


class MarkdownXRefRole(XRefRole):
    """Instantiates the MarkdownXRefRole class."""

    def __call__(self, typ, rawtext, text, lineno, inliner,
                 options={}, content=[]):
        """Special method for calling the class.

        :param self: The current instance.
        :param typ: Presumably a type of something.
        :param rawtext: The raw text to cross reference.
        :param lineno: The line number of the text to reference.
        :param inliner: Is this reference inline.
        :param options: The option spec for the reference.
        :param content: Content of the reference link.
        :return self.result_nodes: Call to method to produce reference nodes.
        """
        env = inliner.document.settings.env
        typ = typ.lower()
        domain, role = typ.split(':', 1)
        classes = ['xref', domain, '%s-%s' % (domain, role)]

        # split title and target in role content
        has_explicit_title, title, target = self.split_explicit_title(
            rawtext, inliner)
        title = utils.unescape(title)
        target = utils.unescape(target)

        # create the reference node
        refnode = self.nodeclass(rawtext, reftype=role, refdomain='std',
                                 refexplicit=has_explicit_title)

        # we may need the line number for warnings
        set_role_source_info(inliner, lineno, refnode)  # type: ignore
        title, target = self.process_link(
            env, refnode, has_explicit_title, title, target)
        # now that the target and title are finally determined, set them
        refnode['reftarget'] = target
        refnode += self.innernodeclass(rawtext, title, classes=classes)
        # we also need the source document
        refnode['refdoc'] = env.docname
        refnode['refwarn'] = self.warn_dangling
        # result_nodes allow further modification of return values
        return self.result_nodes(inliner.document, env, refnode, is_ref=True)

    def split_explicit_title(self, rawtext, inliner):
        """Split reference titles.

        .. todo:: This is too complicated

        :param self: The current instance.
        :param rawtext: The raw text of the title.
        :param inliner: Is this inline or not.
        :return name, target: With a conditional.
        """
        match = inliner.patterns.initial.search(rawtext)
        name = match.group('refname_uri')
        # target = match.group('refuri').strip()
        uri = match.group('refuri').strip()

        index = uri.find('#')  # looking for the 1st occurrence
        if index < 0 or index == len(uri) - 1:
            # URI: ../my-document.md# or. ../my-document.md
            target = uri.rstrip('#').rsplit('.', 1)[0].replace('\\', '/')
        else:
            srcdir = inliner.document.transformer.env.srcdir + path.sep
            if index == 0:
                # URI: #the-example-title
                hash = uri[1:]
                file = inliner.document['source'][len(srcdir):]
            else:
                # URI: ../my-document.md#the-example-title
                cur_doc_dir = path.dirname(inliner.document['source'])
                seg = uri.split('#', 1)
                hash = seg[1]
                file = seg[0][1:] \
                    if seg[0].startswith('/') \
                    else path.normpath(path.join(cur_doc_dir, seg[0]))[len(srcdir):]
            target = file + target_file_sep + hash
            target = nodes.make_id(target.replace('\\', '/'))

        return name != '_', name, target


class MarkdownDomain(Domain):
    """Instantiates the markdown domain."""
    #: Name of the domain.
    name = 'md'
    #: Label of the domain.
    label = 'Default'
    #: Special object types.
    object_types = {}
    #: Special directives.
    directives = {}
    #: Additional roles.
    roles = {'ref': MarkdownXRefRole(innernodeclass=nodes.inline),
             'doc': MarkdownXRefRole(innernodeclass=nodes.inline)}
    #: Starting data.
    initial_data = {}
