# Inverted Indexes via MapReduce

This program is intended to use MapReduce to transform input text into an inverted index for IR systems. It was created for USC's CSCI572. 

This is a toy project and not intended for production; MapReduce is run using local threads. There is up to 8 mapper threads and a single consumer thread. 

The output of the program should be a file containing unigrams and a file containing bigrams. 
