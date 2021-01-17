# Setup

You may set up the scrubber either with conda (recommended) or manually.

### With conda (recommended)
1.  If you haven't already, install [miniconda](https://docs.conda.io/en/latest/miniconda.html)
2.  Run ``conda env create -f environment.yml``
3.  Activate the environment with ``conda activate c14scrub``

### Manual setup
1.  Install [Python 3.7.6 along with Pip](https://www.python.org/downloads/release/python-376/)
2.  Install the required packages by running ``pip install numpy pandas ftfy tqdm pyshp shapely matplotlib pyproj`` in your command line. For more info on installing packages, see [this tutorial](https://packaging.python.org/tutorials/installing-packages/).

# Usage

1.  Ensure that your raw records file is saved using UTF-8 encoding. This can be accomplished by most CSV-handling programs.
2.  Execute the program by running ``python scrub.py in_file.csv out_file.csv`` in the command line, where the input file is the name of the raw records file.

The cleaned records will be saved to your specified filename, a list of unknown lab codes will be saved to ``unknown_codes.csv``, and a list of all deleted records with their reason for removal will be saved to ``graveyard.csv``.

# Independent "remove duplicates" feature

If you need to remove duplicate records from a certain dataset without necessarily running the entire scrubbing process on it, this is achievable through ``removeDuplicates.py``. Simply run ``python removeDuplicates.py infile_name.csv outfile_name.csv`` and the program will run only the duplicate removal subroutine on ``infile_name.csv`` and save the resulting dataset to ``outfile_name.csv``.
