import requests
from requests_oauthlib import OAuth1
import tweepy
import time
import datetime
import pandas as pd
import psycopg2
import sys
import pickle
import numpy as np

#Check Current Twitter API Limits
def check_api_limits(api):
    api_limit_status = api.rate_limit_status()

    resources = api_limit_status['resources']
    resource_names = resources.keys()
    for n, limit_type in enumerate(resource_names):
        try:
            limit = resources[limit_type]
            for key in limit.keys():
                remaining = limit[key]['remaining']
                total_allowed = limit[key]['limit']
                if remaining <total_allowed:
                    reset_epoch = limit[key]['reset']
                    reset_time = datetime.datetime.fromtimestamp(reset_epoch).strftime('%H:%M')
                    print('Resource {} subcat {} has only {} requests remaining until {}.'.format(limit_type,key, remaining, reset_time))
                else:
                    continue
        except:
            print('Hit error at {}, item number: {}'.format(limit_type, n))
    
    print('\n All other Resources have not been used in current time window.')



def get_lists(exemplar_screen_name, list_keywords, api):
    """Specify an exemplar of the desired attribute and relevant list keywords to filter on.
    Ideally, go to twitter and ensure the exemplar screen name is a member of a relevantly (titled) list."""
    #Normalize user Input
    user = api.get_user('@{}'.format(exemplar_screen_name))
    list_keywords = [k.lower() for k in list_keywords]


    #Check Current Rate Limits 
    api_limit_status = api.rate_limit_status()
    remaining_requests = api_limit_status['resources']['lists']['/lists/memberships']['remaining']

    reset_epoch = api_limit_status['resources']['lists']['/lists/memberships']['reset']
    reset_time = datetime.datetime.fromtimestamp(reset_epoch)


    #Acquire Lists 
    at_lists = []
    next_cursor = -1
    for i in range(50):
        #Can we make more requests?
        if remaining_requests > 0:
            remaining_requests -= 1
            response = user.lists_memberships(screen_name=exemplar_screen_name, cursor=next_cursor)
            lists_on = response[0]
            cursor_ids = response[1]
            next_cursor = cursor_ids[1]
            #Filter Lists using Keywords:
            lists_on = [l for l in lists_on if any([lkw in l.name.lower() for lkw in list_keywords])]
            at_lists += lists_on

        else:
            #Check Time Remaining.
            api_limit_status = api.rate_limit_status()
            remaining_requests = api_limit_status['resources']['lists']['/lists/memberships']['remaining']

            reset_epoch = api_limit_status['resources']['lists']['/lists/memberships']['reset']
            reset_time = datetime.datetime.fromtimestamp(reset_epoch)
            
            remaining_time = reset_time - datetime.datetime.now()
            #Sleep for this Amount of Time.
            time.sleep(remaining_time.seconds+2)
            
            #Reset Remaining Requests so that program will continue.
            api_limit_status = api.rate_limit_status()
            remaining_requests = api_limit_status['resources']['lists']['/lists/memberships']['remaining']
            
            continue
    return at_lists


def get_lists_member_counts(attribute_lists, api):
    """#Of all the attribute lists, scrape their members.
    This function returns a dataframe with the screen_names and their corresponding
    frequencies that were found in the attribute lists."""
    
    #Check Current Rate Limits 
    api_limit_status = api.rate_limit_status()
    remaining_requests = api_limit_status['resources']['lists']['/lists/members']['remaining']

    reset_epoch = api_limit_status['resources']['lists']['/lists/members']['reset']
    reset_time = datetime.datetime.fromtimestamp(reset_epoch)


    ex_counts = {}
    for l in attribute_lists:
        #How many members are there?
        needed = l.member_count
        next_cursor = -1
        list_members = []
        needed = l.member_count
        while len(list_members) < needed:
            #Can we make more requests?
            if remaining_requests > 0:
                remaining_requests -= 1
                try:
                    response = l.members(cursor=next_cursor)
                    members = response[0]
                    cursor_ids = response[1]
                    next_cursor = cursor_ids[1]
                    members = [m.screen_name for m in members]
                    list_members += members
                except:
                    print('{} hit error. {} members acquired out of {}'.format(l.name, len(list_members), needed))
                    break
            else:
                #Check Time Remaining.
                api_limit_status = api.rate_limit_status()
                remaining_requests = api_limit_status['resources']['lists']['/lists/members']['remaining']

                reset_epoch = api_limit_status['resources']['lists']['/lists/members']['reset']
                reset_time = datetime.datetime.fromtimestamp(reset_epoch)
                
                remaining_time = reset_time - datetime.datetime.now()
                #Sleep for this Amount of Time.
                time.sleep(remaining_time.seconds+2)
                
                #Reset Remaining Requests so that program will continue.
                api_limit_status = api.rate_limit_status()
                remaining_requests = api_limit_status['resources']['lists']['/lists/members']['remaining']
                
                continue
        for m in list_members:
            ex_counts[m] = ex_counts.get(m, 0) + 1
    mems = pd.DataFrame.from_dict(ex_counts, orient='index')
    mems.columns = ['Count']
    mems = mems.sort_values(by='Count', ascending=False)
    return mems


def get_n_followers(screen_names, n, api, mode='pickle', cur=None):
    """Get screen_name followers.

    Mode determines the save type.
    Default option is 'pickle'; if 'postgres', user must pass psycopg2 cursor for where to save the file.
    Note that this function doesn't return anything but rather saves each screen_names followers to a pickle file."""
    #Each request will return 5000 followers.
    

    api_limit_status = api.rate_limit_status()
    remaining_requests = api_limit_status['resources']['followers']['/followers/ids']['remaining']

    reset_epoch = api_limit_status['resources']['followers']['/followers/ids']['reset']
    reset_time = datetime.datetime.fromtimestamp(reset_epoch)

    for user in screen_names:
        as_of_date = datetime.datetime.now().strftime('%b-%d-%Y')

        sys.stdout.flush()
        sys.stdout.write('\r')
        sys.stdout.write('Started pulling users for {}'.format(user))
        
        #Create List to Store Followers
        user_followers = []
        
        #Ensure that n is not greater than number of followers. Then calculate page depth needed.
        n_followers = api.get_user(screen_name=user).followers_count
        if  n_followers < n:
            page_depth_desired    = n_followers//5000
        else:
            page_depth_desired    = n//5000
        
        #Initialize Cursor
        next_cursor = -1
        one_percent_complete = page_depth_desired//100
        for i in range(page_depth_desired):
            raw_percent = (i+1)/float(page_depth_desired)
            percent_complete = int(round(raw_percent*100,0))
            decile_complete = int(percent_complete//10)
            sys.stdout.flush()
            sys.stdout.write('\r')
            sys.stdout.write("Scraping {}: [{}{}] {}%{}".format(user,'='*decile_complete,' '*(10-decile_complete), percent_complete, ' '*10))
            #Can we make more requests?
            if remaining_requests > 0:
                response = api.followers_ids(screen_name=user, cursor=next_cursor)
                remaining_requests -= 1

                followers = response[0]
                cursor_ids = response[1]
                next_cursor = cursor_ids[1]
                
                #Append Id's to list
                if mode == 'pickle':
                    user_followers += followers
                elif mode == 'postgres':
                    for follower in followers:
                        cur.execute("""INSERT INTO user_follower_ids (screen_name , follower_id, as_of_starttime)
                                   VALUES ((%s), (%s), (%s));""", (user, follower, as_of_date))
            else:
                #Check Time Remaining.
                api_limit_status = api.rate_limit_status()
                remaining_requests = api_limit_status['resources']['followers']['/followers/ids']['remaining']

                reset_epoch = api_limit_status['resources']['followers']['/followers/ids']['reset']
                reset_time = datetime.datetime.fromtimestamp(reset_epoch)
                
                remaining_time = reset_time - datetime.datetime.now()
                #Sleep for this Amount of Time.
                time.sleep(remaining_time.seconds+2)
                
                #Reset Remaining Requests so that program will continue.
                api_limit_status = api.rate_limit_status()
                remaining_requests = api_limit_status['resources']['followers']['/followers/ids']['remaining']
                
                continue
        
        if mode == 'pickle':
            #Save to Incremental Archive File
            output = open('first_{}_follower_ids_for_{}.pkl'.format(n, user), 'wb')
            pickle.dump(user_followers, output)
            output.close()

def load_followers(screen_names,n):
    """Load screen_names followers."""
    user_followers_dict = {}
    for user in screen_names:
        pkl_file = open('first_{}_follower_ids_for_{}.pkl'.format(n, user), 'rb')
        data1 = pickle.load(pkl_file)
        pkl_file.close()
        # print(user, type(data1), len(data1), len(set(data1)))
        user_followers_dict[user] = set(data1)
    return user_followers_dict

def jaccard_scores(screen_names, user_followers_dict):
    """Calculate Jaccard Inices."""
    user_jaccard_similarities = {}
    for n, user1 in enumerate(screen_names):
        temp = {}
        for user2 in screen_names[n+1:]:
            set1 = set(user_followers_dict[user1])
            set2 = set(user_followers_dict[user2])
            overlap = len(set1.intersection(set2))
            union = len(set1.union(set2))
            jaccard = float(overlap)/union
            temp[user2] = jaccard
        user_jaccard_similarities[user1]=temp
    # user_jaccard_similarities_df = pd.DataFrame.from_dict(user_jaccard_similarities, orient='index')
    return user_jaccard_similarities


#Calculate Final Brand Attribute Affinity (0-1).
#This function should not only return the brand attribute affinity of the brand in question,
#but also the brand attribute affinity of some top exemplars for comparison. In the future, these 
#exemplar scores could be used to norm results.
#Note that this should be a weighted average; Jaccard Index * # Occurences in Attribute Lists of Exemplar
def weighted_attribute_scores(screen_names, user_jaccard_similarities, attribute_exemplars):
    """Screen Names will be a list of twitter screen names for both Brands and Exemplars.
    The user_jaccard_similarities is the output from jaccard scores and is a dictionary of dictionaries.
    {User 1: {User2: score, User3: score, ...} ...} attribute_exemplars should be a dataframe with 2 columns:
    screen_name and count giving the frequency of exemplar screen_names from the list scrapes."""
    #1) Create dictionary of Exemplar Weights
    df = attribute_exemplars
    exemplars = list(df[df['count']>=5].screen_name)
    weights = list(df[df['count']>=5]['count'])
    weights_normed = [x/np.sum(weights) for x in weights]
    ex_weights = dict(zip(exemplars, weights_normed))
    #2) Calculate the weighted average.
    brand_attribute_affinities = {}
    for brand in screen_names:
        scores = []
        for ex in exemplars:
            if brand == ex:
                jaccard = 1
            
            else:
                try:
                    jaccard = user_jaccard_similarities[brand][ex]
                except:
                    jaccard = user_jaccard_similarities[ex][brand]
            weight = ex_weights[ex]
            score = weight*jaccard
            scores.append(score)
        brand_attribute_affinities[brand] = np.mean(scores)
    final = pd.DataFrame.from_dict(brand_attribute_affinities, orient='index').reset_index()
    final.columns = ['Brand', 'Attribute Affinity']
    final['Attribute Affinity Normed'] = final['Attribute Affinity']/final['Attribute Affinity'].max()
    return final



def gather_follower_id_details(screen_names, list_name, n=0):
    """Get user_details from a user id.
    
    n is where you are starting from a master list. This can be used to restart crashed runs.

    Current Rendition will make incremental pickle saves roughly every 3 hours.


    *****WARNING*****
    This function can take a very long time to run!
    900 Requests/15min window.
    10000 Requests is roughly every 3 hours.
    10**5 30hrs; 10**6; 12 days. 10 Million; 4 months.
    *****************
    

    List_name should be a description of whom the screen_names were assosciated with (ie they were all followers of NFL)."""
    

    api_limit_status = api.rate_limit_status()
    remaining_requests = api_limit_status['resources']['users']['/users/show/:id']['remaining']

    reset_epoch = api_limit_status['resources']['users']['/users/show/:id']['reset']
    reset_time = datetime.datetime.fromtimestamp(reset_epoch)

    detailed_screen_names = {}
    n_errors = 0

    for n, user in enumerate(screen_names):
        
        raw_percent = (n+1)/float(len(screen_names))
        percent_complete = int(round(raw_percent*100,0))
        decile_complete = int(percent_complete//10)
        sys.stdout.flush()
        sys.stdout.write('\r')
        sys.stdout.write("Scraping {} user details: [{}{}] {}%{} Number of errors: {}".format(list_name,'='*decile_complete,' '*(10-decile_complete), percent_complete, ' '*10, n_errors))

        
        #Incremental Saves; 10000 Requests is roughly every 3 hours.
        #10**5 30hrs; 10**6; 12 days. 10 Million; 4 months.
        #Sample Sizes/Stats will be fun to think about.
        if n!=0 and n%(10**4)==0:
            now = datetime.datetime.now().strftime('%b%d')
            with open('{}_user_details_through_{}_saved_{}.pickle'.format(list_name, n, now), 'wb') as f:
                # Pickle the 'data' dictionary using the highest protocol available.
                pickle.dump(detailed_screen_names, f, pickle.HIGHEST_PROTOCOL)

        #Can we make more requests?
        if remaining_requests > 0:
            try:
                user_object = api.get_user(id=user)
            except:
                e = sys.exc_info()[0]
                n_errors += 1
                if n_errors <=5:
                    try:
                        print(e.reason)
                    except:
                        print(e)
                user_object = e
            
            remaining_requests -= 1

            detailed_screen_names[user] = user_object
            
        else:
            #Check Time Remaining.
            api_limit_status = api.rate_limit_status()
            remaining_requests = api_limit_status['resources']['users']['/users/show/:id']['remaining']

            reset_epoch = api_limit_status['resources']['users']['/users/show/:id']['reset']
            reset_time = datetime.datetime.fromtimestamp(reset_epoch)
            
            remaining_time = reset_time - datetime.datetime.now()
            #Sleep for this Amount of Time.
            time.sleep(remaining_time.seconds+2)
            
            #Reset Remaining Requests so that program will continue.
            api_limit_status = api.rate_limit_status()
            remaining_requests = api_limit_status['resources']['users']['/users/show/:id']['remaining']
            
            continue
