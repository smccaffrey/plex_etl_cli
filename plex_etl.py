

import os 
import sys 
import shutil 
import time
import json
import argparse
import datetime
import PTN

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
parser.add_argument('--cleanup', action = 'store_true')
parser.add_argument('--test', help = 'Test pipeline for Errors, without actually changing things.', action = 'store_true')
parser.add_argument('--insert_test_movies', action = 'store_true')
parser.add_argument('--test_parse', action = 'store_true', help = 'Test if movies can be parsed and renamed.')
parser.add_argument('--test_tv_map', action = 'store_true')


args = parser.parse_args() 

def initialize_plex_info():
	info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'plex_info.json')
	if not os.path.exists(info_file):
		file = open(info_file, 'w')
		json.dump({'GENERATED_AT' : str(datetime.datetime.now())}, file)
		file.close()

def _write_to_plex_info(data: dict):
	info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'plex_info.json')
	with open(info_file, 'r') as old:
		info = json.load(old)
	info.update(data)
	with open(info_file, 'w') as new:
		json.dump(info, new) 

def initialize_movies():
	path_to_movies = input('Enter full path Plex Movie Library: ')
	info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'plex_info.txt')
	while not os.path.exists(path_to_movies):
		print('{} does not exist'.format(path_to_movies))
		path_to_movies = input('Enter full path Plex Movie Library: ')
	data = {'PLEX_MOVIE_LIBRARY' : str(path_to_movies)}
	_write_to_plex_info(data)

def initialize_tvshows():
	path_to_tvshows = input('Enter full path TV Show Library: ')
	data = {}
	while not os.path.exists(path_to_tvshows):
		print('{} does not exist'.format(path_to_tvshows))
		path_to_tvshows = input('Enter full path TV Show Library: ')
	data = {'PLEX_TVSHOWS_LIBRARY' : str(path_to_tvshows)}
	_write_to_plex_info(data)

def _map_tv_shows():
	info = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'plex_info.json')
	with open(info, 'r') as f:
		data = json.load(f)

	print(data['PLEX_TVSHOWS_LIBRARY'], type(data['PLEX_TVSHOWS_LIBRARY']))
	titles = []
	for root, dirs, files in os.walk(data['PLEX_TVSHOWS_LIBRARY']):
		sub_root = root.replace(data['PLEX_TVSHOWS_LIBRARY'], "")
		#print(sub_root)
		print(sub_root.split('/'), len(sub_root.split('/')) == 1)
		if len(sub_root.split('/')) == 1:
			title = "no_title"
		else:
			title = sub_root.split('/')[1]
		titles.append(title)
		#print(title, len(title))
		total_depth = len(root.split('/'))
		sub_depth = len(sub_root.split('/'))
		#print(title, total_depth, sub_depth)
		#for dr in dirs:
		#	print(root + "\t----->" + dr)
	#print(titles)
	#print(data['PLEX_TVSHOWS_LIBRARY'])

def initialize_tvshows_dirs():
	root = 'tvshows'
	sub_dirs = ['1_dump', '2_extracted', '3_transformed', '4_error', '5_encoding_queue']
	for sub_dir in sub_dirs:
		path = os.path.join(os.path.dirname(os.path.realpath(__file__)), root, sub_dir)
		if not os.path.exists(path):
			os.makedirs(path)

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
		for root, dirs, files in os.walk(dump_dir):
			for file in files:
				current = os.path.join(root, file)
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
		if _parsable(movie) and (movie.endswith('.mp4') or movie.endswith('.mkv') or movie.endswith('.avi')):
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

def cleanup():
	movies_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'movies')
	dump_dir = os.path.join(movies_dir, '1_dump')
	extracted_dir = os.path.join(movies_dir, '2_extracted')
	i = 0
	for root, dirs, files in os.walk(dump_dir):
		for _dir in dirs:
			os.rmdir(os.path.join(root, _dir))
			i += 1
	j = 0
	for root, dirs, files in os.walk(extracted_dir):
		for file in files:
			os.unlink(os.path.join(root,file))
			j += 1
	return "\nRemoved {} directories from 1_dump\n\nRemoved {} files from 2_extracted".format(i, j)

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
			test_file_dir = os.path.join(root_path, test_file)
			test_file_path = os.path.join(root_path, test_file, str(test_file) + '.mp4')
			os.mkdir(test_file_dir)
			Path(test_file_path).touch()
		return "Test movies inserted into ETL directories."
	return "Test FAILED. Make sure to run --initialize FIRST!"

def update_from_repo():
	"""Update from github repo
	"""
	return	

if __name__ == '__main__':

	initialize_plex_info()

	if args.pipe == 'movies':
		print('Movies ETL pipeline chosen')

		if args.initialize:
			print('initializing directory structure for staging movies')
			initialize_movies()
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

		if args.cleanup:
			print(cleanup())

	if args.pipe == 'tv_shows':
		print('TV Shows ETL pipeline chosen')

		if args.initialize:
			initialize_tvshows()
			initialize_tvshows_dirs()

		if args.test_tv_map:
			_map_tv_shows()
		


