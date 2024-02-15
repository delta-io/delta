# Code language tabs

The module adds a new directive, `code-language-tabs` to [Sphinx](http://www.sphinx-doc.org/).

Tested with Sphinx 1.8.5.

## Usage

### conf.py

```python
extensions = ['code_language_tabs']
```

### docs source

#### short form

```markdown
.. code-language-tabs::

    ```python
    python code
    ```

    ```java
    java code
    ```

    ```dotnet
    dotnet code
    ```

```

#### mixed form

``` markdown
.. code-language-tabs::

    .. lang:: python
        
        some arbitrary python related content

    .. lang:: java
        
        some arbitrary java related content

    .. lang:: dotnet
        
        some arbitrary .net related content
```

## Notes

* In the `short form` the directive may contain only `literal blocks`. If anything else faced inside the body - an exception would be thrown.
* In the `mixed form` the directive may contain only `lang` children, which represent particular pages. The children themselves may contain any arbitrary content.
* You can not mix `short form` and `mixed form` into a single directive body.
* The blocks order in final HTML is the same as in sources. The directive does not reorder blocks.

## Output mode

The extension might require to function differently when building different outputs. So there is a config setting for changing the extension output mode.

### code-language-tabs-mode

Type: `str`

Default: `html`

Options:

* `html` - produce an output for further handling
* `markdown` - produce an output compatible with Microsoft docs
* `off` - outputs the directive content directly, as there was no directive used

To pass a config value through the command line, see the [command-line options](https://www.sphinx-doc.org/en/1.5/man/sphinx-build.html):

```bash
sphinx-build -D code-language-tabs-mode=markdown <other_options>
```
