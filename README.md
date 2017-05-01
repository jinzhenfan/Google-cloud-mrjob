# Google-cloud-mrjob

MapReduce practice on wikipages.

## Aim: Find the hot points in the wikipage network.

## Method: 

  ** Analyze word frequency and cross-reference of simple, english and thai wikipedia pages (3.5 million pages, 8GB data). 

  ** XML Parsing of contents. 

  ** Use heap structure to accelerate basic stats and aggregation for word counting.

  ** Use reservior sampling for quantile calculation

  ** Run mrjob on local, hadoop and Google Cloud Services. Evaluate the sacalability of algorithm. 
