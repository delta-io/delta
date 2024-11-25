# Markdown Reader

The module teaches [Sphinx](http://www.sphinx-doc.org/) to understand the common markdown format. The module code is much based on the existing Sphinx RST reader, so it also supports a lot of builtin `ReStructured Text` constructs and `Sphinx` rst syntax extensions. 

Tested with Sphinx 1.8.5.

## Usage

### conf.py
```python
source_suffix = {'.md': 'markdown'}
extensions = ['markdown_reader']
```
