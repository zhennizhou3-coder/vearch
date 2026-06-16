# Vearch 中文文档

基于 Sphinx 构建的 Vearch 中文版文档。

## 本地构建

```bash
pip install -r docs/requirements.txt
sphinx-build -b html docs/source docs/build/html
```

## 本地预览

```bash
cd docs/build/html && python3 -m http.server 8080
```
