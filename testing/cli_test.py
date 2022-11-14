
import sys
 
# setting path
sys.path.append('../tap_mongodb')
 
# importing
from tap_mongodb  import main_impl

main_impl()