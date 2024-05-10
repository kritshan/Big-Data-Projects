Hey!

In this notebook, I use spark to develop a Logistic Regression model. The goal of the notebook is to correctly classify whether a document is an Australian court document. In the dataset, there exist about 170,000 different documents. To complete this task, I first use Spark to make a dictionary that contains the 20,000 most frequently occuring words in the training set of documents. Then, I manually convert each document to a TF-IDF vector, or a term frequency–inverse document frequency vector. This format allows use to view a document in terms of frequency of each word. 

Next, I write the gradient function of Logistic Regression and use gradient descent to train the model. 

Lastly, I use the model to predict test documents. I had to convert these documents to TF-IDF vectors in order to run the model on them. 