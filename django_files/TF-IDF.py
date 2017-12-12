from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

#Defining SparkContext
conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

# Loading document and extracting data from it
rawData = sc.textFile(r"E:\files\apache_spark\wikipedia_data.tsv")
fields = rawData.map(lambda x: x.split("\t"))
documents = fields.map(lambda x: x[3].split(" "))

# Storing the document names for later identification of webpage names for displaying our search result
documentNames = fields.map(lambda x: x[1])

# Hashing the words in each document to their term frequencies
hashingTF = HashingTF(100000)  #each words are assigned a number between 0-100K, more efficient
tf = hashingTF.transform(documents)
# print(tf.take(2))
# At this point we have an RDD of sparse vectors representing each document,
# Sparse vectors mean that we have record for available data only not for unavailable data, unlike Dense vectors
# where each value maps to the term frequency of each unique hash value.

# Compute the TF*IDF of each term in each document
tf.cache()	#keeping term frequencies in memory for quick access
idf = IDF(minDocFreq=2).fit(tf)
tfidf = idf.transform(tf)
# print(tfidf.take(2))
# Now we have an RDD of vectors, where each value is the TFxIDF
# of each unique hash value for each document.

# We know, the article for "Abraham Lincoln" is in our data set
# so let's search for "Gettysburg" (Lincoln gave a famous speech there)

# First, let's figure out what hash value "Gettysburg" maps to by finding the
# index a sparse vector from HashingTF gives us back

# gettysburgTF = hashingTF.transform(["Gettysburg"])
gettysburgTF = hashingTF.transform(["avocado"])
gettysburgHashValue = int(gettysburgTF.indices[0])

# Now we will extract the TF*IDF score for Gettsyburg's hash value into
# a new RDD for each document:
gettysburgRelevance = tfidf.map(lambda x: x[gettysburgHashValue])

# We'll zip in the document names so we can see which is which:
zippedResults = gettysburgRelevance.zip(documentNames)

# And, print the document with the maximum TF*IDF value:
# print ("Best document for Gettysburg is:")
print(zippedResults.max())
#print("\n\n")
#print(zippedResults.collect())
#print(zippedResults.take(2),end='<br>')