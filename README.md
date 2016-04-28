## Installation
Here is a step by step guide how to install chip-qc on your system. It will get your required python libraries installed and provides a list of **R** dependencies below.
 
### Requirements
#### Python 
First, [python](https://www.python.org) needs to be installed, if it does not exist yet on the system . Then check, if the package manager [pip](https://pip.pypa.io/en/stable/) is present as well.

|Package | version |
|--------|---------|
|python | > 2.7.5 (but not 3.*) |

Once python and pip are working, you can install the requirements by using:
```
pip install -r requirements.txt
```
or if you don't have permissions to install python libraries, you can install it into your own user libary using **--user**:
```
pip install -r requirements.txt --user
```
The last step to complete the installation of chip-qc is the execution of setup.py using the following command. Add **--user** at the end for the user library: 
```
python setup.py install
```
:exclamation: To reinstall or upgrade the package, you should run:
```
python setup.py install --upgrade
```

#### **R** requirements
The free statistical softer package **R** is used for calculations and graphics in chip-qc.

R (tested for 3.1.2)
RSQLite
reshape
ggplot2
RColorBrewer

From bioconductor:
biocLite("Gviz")
biocLite("rtracklayer")
biocLite("biomaRt")
	
