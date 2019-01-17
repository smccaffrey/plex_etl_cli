### Plex Managing Script
### Extracts, Transforms, and Loads movies from raw torrent
### files into the Plex Movie Database

import os, sys, shutil, time
import argparse
import PTN #Parse-torrent-name package

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
DUMP = CURRENT_DIR + '1_dump/'
EXTRACTED = CURRENT_DIR + '2_extracted/'
TRANSFORMED = CURRENT_DIR + '3_transformed/'
LOADED = CURRENT_DIR + '4_loaded/'
ERROR = CURRENT_DIR + '5_error/'
ENCODER = CURRENT_DIR + '6_encoding_queue/'
MOVIES_DIR = '/mnt/md0/Media/Movies/'

### Must execute scrupt inside RecentlyAdded Directory
"""
parser = argparse.ArgumentParser(description="Script to be ran when a new movie is added to the server.")
parser.add_argument('--movie', dest="movie", metavar="FILE")
args = parser.parse_args()

print(args.movie)
"""




if __name__ == '__main__':

    """New code to replace current code
    media_name = args.movie.split('/')[-1]
    info = PTN.parse(media_name)
    print(info['title'], info['year'])

    new_dir = MOVIES_DIR + info['title'] + " " + "(" + str(info['year']) + ")2" 
    os.mkdir(new_dir)

    shutil.move(media_name, new_dir)
    """

    i = 0
    
    for root, dirs, files, in os.walk(DUMP):
        for file in files:
            if file.endswith(".mp4") or file.endswith(".mkv") or file.endswith(".avi"):
                print("Found and Extracting ..." + "\t" + file)
                i += 1
                current_path = os.path.join(root, file)
                new_path = EXTRACTED + file
                shutil.move(current_path, new_path)
                time.sleep(2)
    print(' Extracted %s media files.' % i)

    j = 0
    k = 0
    for filename in os.listdir(EXTRACTED):
	if filename.endswith('.mp4') or filename.endswith('.m4v'):
		try:
                	info = PTN.parse(filename)
            		newdir = MOVIES_DIR + info['title'] + " " + "(" + str(info['year']) + ")"
            		newfilename = info['title'] + " " + "(" + str(info['year']) + ")" + ".m4v"
            		print("Building ... %s\n" % newfilename)
            		os.mkdir(newdir)
            		time.sleep(2)
            		shutil.move(EXTRACTED + filename, newdir + '/' + newfilename)
            		j += 1
		except:
            		shutil.move(EXTRACTED + filename, ERROR + filename)
            		k += 1
	else:
		shutil.move(EXTRACTED + filename, ENCODER + filename)


    print('Successfully created %s Movies.' % j)
    print('%s Movies queued for encoding.' % (i - j -k))
    print('%s files had some kind of error. Please review.' % k)
    os.chdir('/mnt/md0/Media/Staging/staging_movies/6_encoding_queue')
    os.system('nohup bash encoder.sh &>/dev/null &')
