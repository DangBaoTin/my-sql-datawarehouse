## Install and activate virtual environment
- Init virt env:
```bash
python3 -m venv venv
virtualenv -p /Library/Frameworks/Python.framework/Versions/3.9/bin/python3.9 venv
```
- Activate virtual environment:
```bash
source venv/bin/activate
```
- Install `requirements.txt` for the required libraries:
```bash
pip install -r requirements.txt
```
- To delete any unnecessary libraries:
```bash
pip uninstall <package-name>
```
- To deactivate virtual environment:
```bash
deactivate
rm -rf venv
```

...or create a new repository on the command line
```bash
echo "# my-sql-datawarehouse" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/DangBaoTin/my-sql-datawarehouse.git
git push -u origin main
```

...or push an existing repository from the command line
```bash
git remote add origin https://github.com/DangBaoTin/my-sql-datawarehouse.git
git branch -M main
git push -u origin main
```