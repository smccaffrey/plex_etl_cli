

import os, sys, shutil, time
import PTN
import argparse

from subprocess import call
from pathlib import Path

from tests.files import test_values


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
TESTS = CURRENT_DIR + "tests/"
"""
test_files_movies = [
  "Hercules (2014) 1080p BrRip H264 - YIFY",
  "Dawn.of.the.Planet.of.the.Apes.2014.HDRip.XViD-EVO",
  "22 Jump Street (2014) 720p BrRip x264 - YIFY",
  "Hercules.2014.EXTENDED.1080p.WEB-DL.DD5.1.H264-RARBG",
  "Hercules.2014.Extended.Cut.HDRip.XViD-juggs[ETRG]",
  "Hercules (2014) WEBDL DVDRip XviD-MAX",
  "WWE Hell in a Cell 2014 PPV WEB-DL x264-WD -={SPARROW}=-", 
  "UFC.179.PPV.HDTV.x264-Ebi[rartv]",
  "X-Men.Days.of.Future.Past.2014.1080p.WEB-DL.DD5.1.H264-RARBG",
  "Guardians Of The Galaxy 2014 R6 720p HDCAM x264-JYK",
  "Guardians of the Galaxy (CamRip / 2014)",
  "Brave.2012.R5.DVDRip.XViD.LiNE-UNiQUE",
]
"""
parser = argparse.ArgumentParser(description="Script to be ran when a new movie is added to the server.")
parser.add_argument('--pipe', choices = ['movies', 'tv_shows'], help = 'Specify which ETL pipe to execute.', required = True)
parser.add_argument('--initialize', help = 'Build directory structure required by script', action = 'store_true')
parser.add_argument('--extract', help = 'Extract all media files', action = 'store_true')
parser.add_argument('--transform', action = 'store_true')
parser.add_argument('--test', help = 'Test pipeline for Errors, without actually changing things.', action = 'store_true')
parser.add_argument('--insert_test_movies', action = 'store_true')


args = parser.parse_args()

MOVIES_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies')
DUMP_DIR = os.path.join(MOVIES_DIR, '1_dump')
EXTRACTED_DIR = os.path.join(MOVIES_DIR, '2_extracted')
TRANSOFRMED_DIR = os.path.join(MOVIES_DIR, '3_transformed')
ERROR_DIR = os.path.join(MOVIES_DIR, '4_error')
ENCODING_DIR = os.path.join(MOVIES_DIR, '5_encoding_queue')


def initialize_movies_dirs():
	root = 'movies'
	sub_dirs = ['1_dump', '2_extracted', '3_transformed', '4_error', '5_encoding_queue']
	for sub_dir in sub_dirs:
		path = os.path.join(os.path.dirname(os.path.realpath(__file__)), root, sub_dir)
		if not os.path.exists(path):
			os.makedirs(path)

def extract_movies():
	movies_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies')
	dump_dir = os.path.join(movies_dir, '1_dump')
	extracted_dir = os.path.join(movies_dir, '2_extracted')
	for file in os.listdir(dump_dir):
		current = os.path.join(dump_dir, file)
		new = os.path.join(extracted_dir, file)
		shutil.move(current, new)
	return

def transform_movies():

	j = 0
	k = 0
	for filename in os.listdir(EXTRACTED_DIR):
		if filename.endswith('.mp4') or filename.endswith('.m4v'):
			file_full_path = os.path.join(EXTRACTED_DIR, filename)
			try:
				info = PTN.parse(filename)
				sub_dir_name = info['title'] + " " + "(" + str(info['year']) + ")"
				newdir = os.path.join(TRANSOFRMED_DIR, sub_dir_name)
				#newdir = MOVIES_DIR + info['title'] + " " + "(" + str(info['year']) + ")"
				newfilename = info['title'] + " " + "(" + str(info['year']) + ")" + ".m4v"
				new_file_full_path = os.path.join(newdir, newfilename)
				print("Building ... %s\n" % newfilename)
				os.mkdir(newdir)
				time.sleep(2)
				shutil.move(file_full_path, new_file_full_path)
				j += 1
			except:
				print('error')
				new_file_error_path = os.path.join(ERROR_DIR, filename)
				shutil.move(file_full_path, new_file_error_path)
				k += 1
	return

def insert_test_movies():
	root_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies', '1_dump')
	if not os.path.exists(root_path):
		return "Movie directory '1_dump' does not exist. Need to --initialize"
	for test_file in test_values.test_files_movies:
		test_file_path = os.path.join(root_path, test_file + '.mp4')
		print(test_file_path)
		os.mknod(test_file_path)
	return "Test movies inserted into ETL directories."
	

if __name__ == '__main__':

	if args.pipe == 'movies':
		print('Movies ETL pipeline chosen')

		if args.initialize:
			print('initializing directory structure for staging movies')
			initialize_movies_dirs()

		if args.insert_test_movies:
			insert_test_movies()

		if args.extract:
			print('Exatracting Movies.')
			extract_movies()

		if args.transform:
			print('Transform called')
			transform_movies()

		


