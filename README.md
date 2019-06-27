# Setup

1.  Ensure you have the [latest version of Python and pip](https://www.python.org/downloads/) installed. 
2.  Install the required packages by running ``pip install pandas numpy tqdm ftfy`` in your command line. For more info on installing packages, see [this tutorial](https://packaging.python.org/tutorials/installing-packages/).

# Usage

The program expects the uncleaned dataset to be in the same directory as ``scrub.py`` in a file called ``radiocarbon_raw.csv``. Execute the program by simply running ``python scrub.py`` in the command line. The program will spit out the clean dataset in a file called ``radiocarbon_scrubbed.csv`` and a list of unknown lab codes in ``unknown_codes.csv``. 
