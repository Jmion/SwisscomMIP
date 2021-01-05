# Instalation

open the environment file to set to path where you want the environment to be installed.

conda env create -f environment.yml

## Setup private access information
conda env config vars set CLIENT_ID=<swisscom client id>
conda env config vars set CLIENT_SECRET=<swisscom client secret>
