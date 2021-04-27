python3 -m pip install --user --upgrade twine

python setup.py bdist_wheel && python setup.py sdist
twine upload dist/*
