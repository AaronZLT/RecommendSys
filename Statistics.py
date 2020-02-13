from pyspark import SparkContext 
inputFile = '/home/zkpk/Desktop/Code/pythonproject/ml-100k/u.user'   
user_data  = SparkContext('local', 'Statistics')

user_fields = user_data.map(lambda line: line.split("|"))

num_users = user_fields.map(lambda fields: fields[0]).count()

num_genders = user_fields.map(lambda fields: fields[2]).distinct().count()

num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()

num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()
print  "user number:  ",num_users


 

