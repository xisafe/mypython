import pandas as pd # load csv's (pd.read_csv)
import numpy as np # math (lin. algebra)
import sklearn as skl # machine learning
from sklearn.ensemble import RandomForestClassifier
#数据来源https://www.kaggle.com/c/digit-recognizer/data?train.csv
import matplotlib.pyplot as plt # plot the data
import seaborn as sns # data visualisation
sns.set(color_codes=True)
#% matplotlib inline
# load data as Pandas.DataFrame
train_df = pd.read_csv('D:/pydata/train.csv')
train_data = train_df.values
test_df = pd.read_csv('D:/pydata/test.csv')
test_data = test_df.values
plt.figure(figsize=(12,8))
sns.countplot(x='label', data=train_df)
plt.title('Distribution of Numbers')
plt.xlabel('Numbers');
# Holdout ( 2/3 to 1/3 )
num_features = train_data.shape[0] # number of features
print("Number of all features: \t\t", num_features)
split = int(num_features * 2/3)
train = train_data[:split]
test = train_data[split:]
print("Number of features used for training: \t", len(train), 
      "\nNumber of features used for testing: \t", len(test))
# Classifier
clf = RandomForestClassifier(n_estimators=100) # 100 trees
# train model / ravel to flatten the array structure from [[]] to []
model = clf.fit(train[:,1:], train[:,0].ravel())
# evaluate on testdata
output = model.predict(test[:,1:])
# calculate accuracy
acc = np.mean(output == test[:,0].ravel()) * 100 # calculate accuracy
print("The accuracy of the pure RandomForest classifier is: \t", acc, "%")
# Classifier
clf = RandomForestClassifier(n_estimators=100) # 100 trees
# train model / ravel to flatten the array structure from [[]] to []
target = train_data[:,0].ravel()
train = train_data[:,1:]
model = clf.fit(train, target)
# modify the test_data, so the number of attributes matchwith the training data (missing label column)
# evaluate on testdata
output = model.predict(test_data)
pd.DataFrame({"ImageId": range(1, len(output)+1), "Label": output}).to_csv('out.csv', index=False, header=True)