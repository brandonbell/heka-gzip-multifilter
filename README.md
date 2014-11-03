heka-gzip-multifilter
=========
Heka Filter to combine streams separately based on a tag and Gzip them. [Mozilla Heka](http://hekad.readthedocs.org/)

GzipMultiFilter
==========
GzipMultiFilter aggregates streams separately based on a defined tag and Gzips them after a configurable time / size is hit.  
This plugin should be treated as experimental as it has not been tested in a high volume environment.  

You will need to balance flush_interval and flush_bytes based on the number of different tags you are going to be aggregating.  After flush_interval is hit, all references with a size > 0 are cleared to ensure stale data is not left around.  

Note: Each payload that is output will add Field[Partition] = `field_tag`.  (i.e. Field[Partition] = '2014-01-01' if stream is being split by date) 

Config:
- flush_interval: Interval in millseconds in which ALL sets of accumulated streams are closed and sent downstream. (default 1000)
- flush_bytes: When a particular set of accumulated streams reaches this (compressed) size it is closed and sent downstream. (default 10). 
- gzip_tag: Tag to indicate payload is a gzipped byte array (default 'compressed') 
- field_tag: Field to aggregate into separate Gzipped streams. 

Example:

        [gzip_multi_filter]
        type = "GzipMultiFilter"
        message_matcher = "Type == 'syslog'"
        gzip_tag = "syslog-gzipped"
        field_tag = "date" 
        flush_interval = 300000 
        flush_bytes = 10000000


