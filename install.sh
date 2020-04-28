rm -rf build
rm -rf dist
rm -rf mindsdb_server.egg-info
# python3 setup.py --help-commands

echo "mode (prod/dev)?"

read mode

if [ "$mode" = "prod" ]; then

    python3 setup.py develop --uninstall
    python3 setup.py clean
    python3 setup.py build
    #python3 setup.py install
    #python3 setup.py bdist_egg -p win32
    python3 setup.py sdist bdist_wheel

    echo "Do you want to publish this version (yes/no)?"

    read publish

    if [ "$publish" = "yes" ]; then
        echo "Publishing mindsdb_client to Pypi"
        python3 -m twine upload dist/*
    fi


fi

if [ "$mode" = "dev" ]; then
    pip3 uninstall mindsdb_client
    pip3 uninstall mindsdb_client --user
    python3 setup.py develop --uninstall
    python3 setup.py develop
fi



