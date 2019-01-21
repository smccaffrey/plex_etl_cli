

import os, sys, shutil, time
import PTN
import argparse

from subprocess import call
from pathlib import Path

from tests.files import test_values

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
TESTS = CURRENT_DIR + "tests/"

parser = argparse.ArgumentParser(description="Script to be ran when a new movie is added to the server.")
parser.add_argument('--pipe', choices = ['movies', 'tv_shows'], help = 'Specify which ETL pipe to execute.', required = True)
parser.add_argument('--initialize', help = 'Build directory structure required by script', action = 'store_true')
parser.add_argument('--extract', help = 'Extract all media files', action = 'store_true')
parser.add_argument('--transform', action = 'store_true')
parser.add_argument('--load', action = 'store_true')
parser.add_argument('--test', help = 'Test pipeline for Errors, without actually changing things.', action = 'store_true')
parser.add_argument('--insert_test_movies', action = 'store_true')
parser.add_argument('--test_parse', action = 'store_true', help = 'Test if movies can be parsed and renamed.')

args = parser.parse_args() 

def initialize():
	user_input = input('Enter full path Plex Movie Library: ')
	info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'plex_info.txt') 
	while not os.path.exists(user_input):
		print('{} does not exist'.format(user_input))
		user_input = input('Enter full path Plex Movie Library: ')
	file = open(info_file, 'w')
	file.write('PLEX_MOVIE_LIBRARY={}'.format(user_input))
	file.close()

def initialize_movies_dirs():
	root = 'movies'
	sub_dirs = ['1_dump', '2_extracted', '3_transformed', '4_error', '5_encoding_queue']
	for sub_dir in sub_dirs:
		path = os.path.join(os.path.dirname(os.path.realpath(__file__)), root, sub_dir)
		if not os.path.exists(path):
			os.makedirs(path)

def _check_movies_initialize():
	root_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies')
	if not os.path.exists(root_path):
		return False
	return True

def extract_movies():
	if _check_movies_initialize():
		movies_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies')
		dump_dir = os.path.join(movies_dir, '1_dump')
		extracted_dir = os.path.join(movies_dir, '2_extracted')
		i = 0
		for file in os.listdir(dump_dir):
			current = os.path.join(dump_dir, file)
			new = os.path.join(extracted_dir, file)
			shutil.move(current, new)
			i += 1
		return "\n{} movies found, and moved to 2_extracted.\n".format(i)
	return "\nExtract FAILED. Make sure to run --initialize FIRST!\n"

def transform_movies():
	extracted = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies', '2_extracted')
	transformed = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies', '3_transformed')
	errored = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies', '4_error')
	i, j = 0, 0
	for movie in os.listdir(extracted):
		old_path = os.path.join(extracted, movie)
		if _parsable(movie):
			title = _parse_movie(movie)
			old_path = os.path.join(extracted, movie)
			new_path = os.path.join(transformed, title)
			shutil.move(old_path, new_path)
			i += 1
			continue
		old_path = os.path.join(extracted, movie)
		new_path = os.path.join(errored, movie)
		shutil.move(old_path, new_path)
		j += 1
	return "\n{} movies TRANSOFRMED. {} movies ERRORED.".format(i, j)

def load_movies():
	assert _is_movie_library_known(), 'Need to Specify PLEX_MOVIE_LIBRARY.'
	movie_lib_loc = _get_movie_library()
	transformed = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies', '3_transformed')
	i = 0
	for movie in os.listdir(transformed):
		print(movie)
		old_full_path = os.path.join(transformed, movie)
		dir_name = os.path.join(movie_lib_loc, movie.split('.')[0])
		os.mkdir(dir_name)
		new_full_path = os.path.join(dir_name, movie)
		shutil.move(old_full_path, new_full_path)
		i += 1
	return "\n{} movies created.".format(i)

def _is_movie_library_known():
	info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'plex_info.txt')
	file = open(info_file, 'r')
	for line in file:
		if line.split("=")[0] == 'PLEX_MOVIE_LIBRARY':
			return True
	return False

def _get_movie_library():
	info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'plex_info.txt')
	file = open(info_file, 'r')
	for line in file:
		if line.split("=")[0] == 'PLEX_MOVIE_LIBRARY':
			return line.split("=")[1]

def _parsable(title):
	info = PTN.parse(title)
	i,j = 0,0
	try:
		if info['title'] and info['year']:
			i += 1
			return True
	except Exception as e:
		if isinstance(e, KeyError):
			return False

def _parse_movie(title):
	info = PTN.parse(title)
	ext = title.split('.')[-1]
	new_title = info['title'] + " " + "(" + str(info['year']) + ")" + "." + ext
	return new_title

def test_movies_parse():
	root_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies', '2_extracted')
	tcnt, fcnt = 0, 0
	passed, failed = [], []
	for file in os.listdir(root_path):
		if _parsable(file):
			tcnt += 1
			passed.append(file)
		else:
			fcnt += 1
			failed.append(file)
	return "\n{} movies tested. {} PASSED / {} FAILED\n\nFAILED LIST: {}\n".format(tcnt + fcnt, tcnt, fcnt, failed)

def insert_test_movies():
	root_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies', '1_dump')
	if _check_movies_initialize():
		for test_file in test_values.test_files_movies:
			test_file_path = os.path.join(root_path, str(test_file) + '.mp4') 
			os.mknod(test_file_path)
			print(test_file)
		return "Test movies inserted into ETL directories."
	return "Test FAILED. Make sure to run --initialize FIRST!"
	

if __name__ == '__main__':

	if args.pipe == 'movies':
		print('Movies ETL pipeline chosen')

		if args.initialize:
			print('initializing directory structure for staging movies')
			initialize()
			initialize_movies_dirs()

		if args.insert_test_movies:
			insert_test_movies()

		if args.extract:
			#print('Exatracting Movies.')
			print(extract_movies())

		if args.test_parse:
			print(test_movies_parse())

		if args.transform:
			print(transform_movies())

		if args.load:
			print(load_movies())

		


