#!/usr/bin/env python
# coding: utf-8

# # Project: Wrangling and Analyze Data Report ( WeRateDogs tweet Data Archive)

# ### import necessary packages
# 
# 

# In[25]:


import re
from IPython.display import Image
import pandas as pd
import numpy as np
import requests
import io
import tweepy
from tweepy import OAuthHandler
import json
from timeit import default_timer as timer
from IPython.display import Image
import matplotlib.pylab as plt
import seaborn as sns
get_ipython().run_line_magic('matplotlib', 'inline')


# 2. Load data

# In[26]:


# WeRateDogs tweets archived data
twitter_archive_df = pd.read_csv('twitter-archive-enhanced.csv')


# In[27]:


# Image predictation data on the same WeRateDog tweets
req = requests.get("https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv")
image_prediction_df =  pd.read_csv(io.StringIO( req.text),sep='\t')


# 3. Use the Tweepy library to query additional data via the Twitter API (tweet_json.txt)

# In[28]:


# Query Twitter API for each tweet in the Twitter archive and save JSON in a text file
# These are hidden to comply with Twitter's API terms and conditions
consumer_key = ''
consumer_secret = ''

access_token = ''
access_secret = ''

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

tweet_ids = twitter_archive_df.tweet_id.values
len(tweet_ids)

# Query Twitter's API for JSON data for each tweet ID in the Twitter archive
count = 0
fails_dict = {}
start = timer()
# Save each tweet's returned JSON as a new line in a .txt file
with open('tweet_json.txt', 'w') as outfile:
    # This loop will likely take 20-30 minutes to run because of Twitter's rate limit
    for tweet_id in tweet_ids:
        count += 1
        print(str(count) + ": " + str(tweet_id))
        try:
            tweet = api.get_status(tweet_id, tweet_mode='extended')
            print("Success")
            json.dump(tweet._json, outfile)
            outfile.write('\n')
        except tweepy.errors.NotFound as e:
            print("Fail")
            fails_dict[tweet_id] = e
            pass
        except tweepy.errors.BadRequest as e:
            print("bad request")
            break;
end = timer()
print(end - start)
print(fails_dict)


# load the json file and transform it to a pandas dataframe

# In[29]:


tweet_id = []
retweet_count = []
favourite_count = []
with open('tweet-json.txt','r') as f:
    for line in f:
        string = f.readline()
        data = json.loads(string)
        tweet_id.append(data['id'])
        retweet_count.append(data['retweet_count'])
        favourite_count.append(data['favorite_count'])

api_df = pd.DataFrame({'tweet_id':tweet_id,'retweet_count':retweet_count,'favourite_count':favourite_count})


# ## Assessing Data
# ### Assess Twitter_archive_df
# 

# In[30]:


twitter_archive_df.head()


# In[31]:


twitter_archive_df.info()


# In[32]:


twitter_archive_df.sample(20)


# In[33]:


# 0btain  count of unique number
twitter_archive_df.nunique()


# In[34]:


twitter_archive_df.source.unique()


# In[35]:


twitter_archive_df.rating_denominator.value_counts()


# In[36]:


twitter_archive_df.query('rating_denominator < 10')


# In[37]:


twitter_archive_df.name.unique()


# ### Assess image_prediction_df data

# In[38]:


image_prediction_df.info()


# In[39]:


image_prediction_df.describe()


# #### Access Api_df

# In[40]:


api_df.head()


# In[41]:


api_df.info()


# ### Quality issues
# #### twitter_archive_df
# 1. Missing values in in_reply_to_status_id,in_reply_to_user_id,retweeted_status_id, retweeted_status_user_id,retweeted_status_timestamp and expanded_urls features
# 2. remove rows in retweet_count and favorite_count with missing values
# 3.  745 dogs have no names(None) 
# 4. Name like a,an,by,his,old,my,O,such,not,one,this,all,the given to some dogs are likely to be errornous
# 6. 78 replies need to be removed as they are not relevant for the analysis
# 7. 181 retweet need to be removed also as they are not  useful for the analysis
# 8. timestamp feature is an object instead of DateTime data type
# 9. expanded_urls contains link which are not valid, possible because they are expired
# 10. Tweet_id 835246439529840640 will throw a zero division error, the rating_denominator is zero.
# 11. Source feature has 4 unique values which are string representation of an HTML anchor element. Both the anchor link and text has little or no insight to offer.
# 
# #### image_prediction_df
# 9. samples of non dog predictions like desktop computers
# 

# ### Tidiness issues
# 
# #### twitter_archive_df
# 1. Multiple ccolumns [“doggo”, “flooter”, “pupper”, “puppo”] for one “stage” column
# 2. Entries of both retweets and reply; (in_reply_to_status_id,in_reply_to_user_id,retweeted_status_id, retweeted_status_user_id and retweeted_status_timestamp) features
# 3. We don't have an actual rating feature or column.
# 
# 
# #### image_prediction_df
# Multiple prediction columns; p1,p2,p3

# ## Cleaning Data
# 

# In[42]:


# Make copies of original pieces of data

clean_twitter_archive = twitter_archive_df.copy()
clean_image_prediction = image_prediction_df.copy()
Clean_api_df = api_df.copy()


# In[43]:


clean_twitter_archive.head()


# ### Issue
# The data contain retweets and replies
# 
# ### Define
# Remove retweets and replies
# 
# ### Code

# In[44]:


#To remove retweets
clean_twitter_archive = clean_twitter_archive[clean_twitter_archive.retweeted_status_id.isnull()]

#To remove replies
clean_twitter_archive = clean_twitter_archive[clean_twitter_archive.in_reply_to_status_id.isnull()]


# ###  Test

# In[45]:


clean_twitter_archive.info()


# #### Issue
# ##### Entries of both retweets and reply; (in_reply_to_status_id,in_reply_to_user_id,retweeted_status_id, retweeted_status_user_id and retweeted_status_timestamp) features
# 
# ####  Define
# Drop the unneed columns

# ##### Code

# In[46]:


#drop the unneeded columns
clean_twitter_archive.drop(['in_reply_to_status_id','in_reply_to_user_id','retweeted_status_id','retweeted_status_user_id',
                           'retweeted_status_timestamp'], axis = 1, inplace=True)


# #### Test

# In[47]:


clean_twitter_archive.sample(5)


# ####  Issue
# #####  Multiple columns [“doggo”, “floofer”, “pupper”, “puppo”] for one “stage” column
# 
# ####  Define
# 
# * Create a stage column and assign one level/stage to each row in order of development. 
#   Example; assign doggo to a row that has both doggo and puppo and
#   None where doggo, floffer,pupper and puppo are None
#   It will be more appropriate to identify each dog the highest level of development. 
#   
# 
# #### Code

# In[48]:


#merge the four columns
clean_twitter_archive['stage'] = (clean_twitter_archive['doggo'].values + clean_twitter_archive['floofer'].values +
                                   clean_twitter_archive['pupper'].values +clean_twitter_archive['puppo'].values)
#descending order of development
stage_order = ['doggo','puppo','pupper','floofer','None']
#stage series
stages = clean_twitter_archive['stage'].values
#assign just one stage according to heriach
for index,value in enumerate(stages):
    for stage in stage_order:
        if stage in value:
            stages[index] = stage
            break;
#reassign stage column          
clean_twitter_archive['stage'] = stages
    


# #### Test

# In[49]:


clean_twitter_archive.sample(5)


# #### Issue
# #####  Multiple columns [“doggo”, “floofer”, “pupper”, “puppo”] for one “stage” column
# 
# #### Define
# * Drop doggo, floffer,pupper and puppo columns

# #### Code 

# In[50]:


clean_twitter_archive.drop(columns=['doggo','puppo','pupper','floofer'], axis = 1, inplace = True)


# #### Test

# In[51]:


clean_twitter_archive.head()


# In[52]:


clean_twitter_archive.info()


# #### Issue
# #### Missing Values
# We hav missing values in expanded_urls
# 
# 
# #### Define
# * Drop the missing values
# 
# 
# #### Code 

# In[53]:


# drop missiing values
clean_twitter_archive.dropna(inplace=True)


# #### Test
# 
# To confirm the result of the code above
# 

# In[54]:


clean_twitter_archive.info()


# ##### Issue
# Tweet_id 835246439529840640 will throw a zero division error, the rating_denominator is zero.
# Rating_denominator equal to zero will throw out error
# 
# #### Define  
# Confirm if the row tweet_id 835246439529840640 still exist as previous cleaning suppose to have removed it
# Confirm if  rating_denominator equal to zero still exist
# 
# #### Code

# In[55]:


clean_twitter_archive.query('tweet_id == 835246439529840640')


# #### Test

# In[56]:


clean_twitter_archive.query('rating_denominator < 1')


# In[57]:


clean_twitter_archive.query('rating_denominator < 10')


# #### Issue 
# ##### We don't have an actual rating feature or column.
# 
# #### Define
# * Create a rating feature (rating_numerator/rating_denominator)

# #### Code

# In[58]:


clean_twitter_archive['rating'] = clean_twitter_archive['rating_numerator'] /  clean_twitter_archive['rating_denominator']


# #### Test

# In[59]:


clean_twitter_archive.head()


# 
# #### Issue
# 
# #### Source feature has 4 unique values which are string representation of an HTML anchor element.
# 
# #### Define
# * Drop source feature

# #### Code

# In[60]:


clean_twitter_archive.drop(columns=['source'], axis = 1, inplace = True)


# #### Test

# In[61]:


clean_twitter_archive.head()


# #### Issue
# 
# #### Timestamp feature is an object instead of DateTime data type
# 
# #### Define
# 
# * Convert timestamp to datetime

# #### Code

# In[62]:


clean_twitter_archive['timestamp']= pd.to_datetime(clean_twitter_archive['timestamp'])


# #### Test 

# In[63]:


clean_twitter_archive.info()


# #### Issues
# 1. 745 dogs have no names(None) 
# 2. Names like a,an,by,his,old,my,O,such,not,one,this,all,the given to some dogs are likely to be errornous
# 
# #### Define
# * Replace (a,an,by,his,old,my,O,such,not,one,this,all,the) with None

# #### Code 

# In[64]:


clean_twitter_archive['name'].replace(['a','an','by','his','old','my','O',
                                       'such','not','one','this','all','the'],'None',inplace=True)


# #### Test

# In[65]:


clean_twitter_archive.query('name == "an"')


# In[66]:


clean_twitter_archive.info()


# ### Issue
#  Samples of non dog predictions
#  
# 
# ### Define
# * Filter out non dog predictions

# ### Code

# In[67]:


clean_image_prediction = clean_image_prediction.query('p3_dog == True')


# ### Test

# In[68]:


clean_image_prediction.query('p3_dog == False')


# ### Issue

#  Multiple prediction columns; p1,p2,p3
# p3 has the highest confidence score

# ### Define

# discard other predictions 

# ### Code

# In[69]:


clean_image_prediction.drop(columns=['img_num','p1','p1_conf',
                                     'p1_dog','p2','p2_conf','p2_dog','p3_dog'],
                            axis = 1, inplace = True)


# ### Test 

# In[70]:


clean_image_prediction.head()


# 
# ### Merge clean_twitter_archive with clean_image_prediction

# In[71]:


master_df = clean_twitter_archive.merge(clean_image_prediction,left_on= 'tweet_id',right_on='tweet_id')


# ### Examine the data 

# In[72]:


master_df


# ### Issue
# ### p3, timestamp and p3_conf features names are not descriptive
# 
# ### Define
# ### Rename p3, timestamp and p3_conf features to be more descriptive

# ### Code

# In[73]:


master_df.rename({'p3':'predicated_specie','timestamp':'date','p3_conf':'prediction_confidence'},inplace=True,axis=1)


# ### Test 

# In[74]:


master_df


# In[75]:


master_df.info()


# ### Merge master_df with api_df

# In[76]:


master_df = master_df.merge(api_df,left_on= 'tweet_id',right_on='tweet_id')


# ### Assess

# In[77]:


master_df.info()


# In[78]:


master_df


# In[79]:


master_df.describe()


# In[80]:


master_df.nunique()


# ## Storing Data
# Save gathered, assessed, and cleaned master dataset to a CSV file named "twitter_archive_master.csv".

# #### Save master_df

# In[81]:


master_df.to_csv('twitter_archive_master.csv',index=False)


# In[82]:


# let's just load the saved version
df = pd.read_csv('twitter_archive_master.csv',parse_dates=['date'])


# In[83]:


df.info()


# ### Create function 

# In[84]:


def get_percentage(series):
    '''Takes in a series/list and returns the percentage
        of each element to the summation of the list/series elements'''
    
    return round((series/np.sum(series)) * 100,2)

def plot_graph(df,graph_type, title ,xlabel, ylabel, ylim):
    '''Takes in a datafrme and plots either bar or pie chart

        Paramenters
        df: Dataframe
        graph_type: str
        title: str
        xlabel: str
        ylabel: str
        ylim: boolean
    
    '''
    
    if graph_type == "bar":
        df.plot.bar(rot=0,width=0.8,edgecolor="black",color=['orange','lightblue','green','yellow','gold'])
    elif graph_type == 'pie':
        df.plot.pie(subplots=True) 
        
    if ylim and graph_type != "pie" :
        plt.ylim(0,100)
        
    # Add title and format it
    plt.title(title.title(),
               fontsize = 14, weight = "bold")
    # Add x label and format it
    plt.xlabel(xlabel.title(),fontsize = 10, weight = "bold")
    # Add y label and format it
    plt.ylabel(ylabel.title(),
               fontsize = 10, weight = "bold")
    

def feature_uniques_percentage(feature_name,graph="bar",ylim= True,
                               cal_percentage = get_percentage,graph_fun = plot_graph,df=df):
    '''Takes in string feature_name and other optional arguments 
        and plots either bar or pie chart of the feature unique
        values percentages

        Paramenters
        feature_name: str
        graph: str -- optional -- default: bar
        ylim: boolean -- optional -- default: True
        cal_percentage: function -- optional -- default: global scope get_percentage function
        graph_fun: function -- optional -- default: global scope plot_graph function
        df: dataframe -- optional -- default: global scope df
    '''
    #  get the counts of the feature unique values
    feature_df = df[feature_name].value_counts().to_frame()
    
    #  prints feature counts
    index = feature_df.index
    print(f"{feature_name} Counts --- ",end=" ")
    for name in index:
        print(f'{name} : { feature_df.loc[name][feature_name]};',end=" ")
        
    #  changes unique values counts to percentage
    #  using the passed in percentage function: cal_percentage
    feature_df[feature_name] = cal_percentage(feature_df[feature_name])
    
    #  print out feature percentages
    print()
    print(f"{feature_name} percentage --- ",end=" ")
    for name in index:
        print(f'{name} : { feature_df.loc[name][feature_name]}%;',end=" ")
    

    title = f"unique values percentages of {feature_name} feature " 
    #   call the graph function
    graph_fun(feature_df,graph,title,feature_name,'Percentage',ylim)
    
    
    
def to_string(name):
    return data[name].to_string(index=False)

def details_fun(data):
    img_url = str(data['jpg_url'])
    [url] = re.findall('https:.*',img_url)
    print(f'Name: {to_string("name")} ; Stage: {to_string("stage")}')
    print(f'Predicted Specie: {to_string("predicated_specie")}')
    print(f'Prediction Confidence: {to_string("prediction_confidence")}')
    print(f'Retweet Count: {to_string("retweet_count")} ; Favourite Count: {to_string("favourite_count")}')
    return url


# ## Analyzing and Visualizing Data

# ### Dog stage distribution

# In[90]:


feature_uniques_percentage('stage')


# Dogs that were not categorized under any stage had the largest percentage. The data is screwed.

# ### Dog with highest rating

# In[91]:


# Dog with highest rating
highest_rating = df.rating.max()
data = df.query('rating == @highest_rating')    
# get the image
Image(details_fun(data), width=300, height=300)


# ### Dog with highest retweet

# In[92]:


# Dog with highest retweet
highest = df.retweet_count.max()
data = df.query('retweet_count == @highest')  
# get details
details_fun(data)


# The image url is invalid

# ### Retweet count and favourite count trend in years

# In[88]:


data = df.groupby(df.date.dt.year)["retweet_count","favourite_count"].sum()
data


# In[89]:


# plot the graph
data.plot(rot=0,figsize=(14,4));
# Add title and format it
plt.title('retweet count and favourite count trend in years '.title(),
               fontsize = 14, weight = "bold");
# Add x label and format it
plt.xlabel('Year'.title(),fontsize = 10, weight = "bold");
# Add y label and format it
plt.ylabel("Count (unit : million)".title(),
               fontsize = 10, weight = "bold");


# From the graph above, their posts receive more like that retweet. The likes reach maximum in 2017 while retweet obtain maximum at the first quarter of 2016 and  continue to decline .
