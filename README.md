## Installation
Here is a step by step guide how to install **chip-qc** on your system. It will get your required python libraries installed and provides a list of **R** dependencies below.
 
### Requirements
This software package is written in the programming language [**Python**](https://www.python.org) and the statistical functional language [**R**](https://www.r-project.org). Extra extensions are required for processing in addition to the basic installion of both packages. Instruction on how to install **chip-qc** and the required extensions are listed below.
#### Python 
First, [**Python**](https://www.python.org) needs to be installed, if it does not exist yet on the system . Then check, if the package manager [**pip**](https://pip.pypa.io/en/stable/) is present as well.

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

#### **R** requirements
The statistical software package [**R**](https://www.r-project.org) is used for calculations and graphics in **chip-qc**. To start with, [**R**](https://www.r-project.org) and the package manager [**Bioconductor**](https://bioconductor.org) needs to be installed, if it does not exist on your system.  

|Package | version |
|--------|---------|
| **R** | 3.1.2 |

Once **R** and **Bioconductor** are working on your system, you can install the requirements by using the commands below, following the instruction on you screen:
```
R
install.packages(c("RSQLite","reshape","ggplot2","RColorBrewer"))
```
The **Bioconductor** packages can be installed using the commands below:
```
R
source("https://bioconductor.org/biocLite.R")
biocLite("Gviz")
biocLite("rtracklayer")
biocLite("biomaRt")
```

### Installation
After installing the requirements, the last step to complete the **chip-qc** installation is the execution of setup.py. This can be done by using the command below - adding **--user** at the end as required for the user library: 
```
python setup.py install
```
:exclamation: To reinstall or upgrade the package, you need to run install again:
```
python setup.py install
```
