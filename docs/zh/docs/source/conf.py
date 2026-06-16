# -*- coding: utf-8 -*-
project = 'zh_docs'
copyright = '2019, vearch'
author = 'vearch'

version = u''
release = '0.1'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx_copybutton'
]

source_suffix = '.rst'
master_doc = 'index'
language = u'zh_CN'
exclude_patterns = []
pygments_style = None

html_theme = 'sphinx_rtd_theme'
htmlhelp_basename = 'vearch Doc'

latex_elements = {}

texinfo_documents = [
    (master_doc, 'Vearch', u'Vearch Documentation',
     author, 'Vearch', 'One line description of project.',
     'Miscellaneous'),
]

epub_exclude_files = ['search.html']

copybutton_visibility = "visible"
