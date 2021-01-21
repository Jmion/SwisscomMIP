# Instalation

Open the environment file to set to the path where you want the environment to be installed.

```
conda env create -f environment.yml
```

## Setup private access information
```
conda env config vars set CLIENT_ID=<swisscom client id>
conda env config vars set CLIENT_SECRET=<swisscom client secret>
```

# Usage

The `dataFetcher.py` is in charge of using the Swisscom MIP API to request the data. By default, it will request the data for the day of the free trial but the code is easily modifiable to allow any day to be queried.

The `SwisscomAnalysis.ipynb` is a notebook showing a few types of analysis that are possible with the data that I collected.
