# Setup

1.  Ensure you have the [latest version of Python and pip](https://www.python.org/downloads/) installed. 
2.  Install the required packages by running ``pip install pandas numpy tqdm ftfy`` in your command line. For more info on installing packages, see [this tutorial](https://packaging.python.org/tutorials/installing-packages/).

# Usage

1.  Ensure the uncleaned dataset is named ``radiocarbon_raw.csv`` and in the same directory as ``scrub.py``. 
2.  Ensure that ``radiocarbon_raw.csv`` is saved using UTF-8 encoding. This can be accomplished by most CSV-handling programs.
3.  Execute the program by simply running ``python scrub.py`` in the command line.

The program will spit out the clean dataset in a file called ``radiocarbon_scrubbed.csv`` and a list of unknown lab codes in ``unknown_codes.csv``. 

# TODO

The following features are yet to be implemented:
    * County centroids for US/Canada dates
