# authentication

from twitter_authentication import bearer_token

# define range of the search

start_date = "21/03/2006"
end_date = "14/09/2022"

start_time = "00:00:00"
end_time = "00:59:00"

# to harvest a particular user's tweets, set source = "from: username"
# example: "from: markhamill"

# to harvest a particular hashtag, set source = "#hastag" 
# example: "#starwars"

# omit the # symbol to just use keywords instead of hashtags

# use OR to harvest from multiple sources at once
# example: "#starwars OR #mandalorian OR #rogueone"
# example: "from: markhamill OR from: starwars OR #starwars"

# use -is:retweet after source to omit retweet

source = ""

hashtag = source

# set output directory and date/time format

output_directory = ""

date_format="DD-MM-YYY"
time_format="HH:MM:SS"


def parameters(start_date, end_date, start_time, end_time, hashtag):
    query_params = {
        'start_time': f'{start_date}T{start_time}Z', # API DATE/HOUR FORMAT = YYYY-MM-DDTHH:MM:SSZ
        'end_time': f'{end_date}T{end_time}Z',
        'tweet.fields': 'author_id,created_at,in_reply_to_user_id,possibly_sensitive,public_metrics,lang,source,entities,geo',
        'max_results': 450,
        'expansions': 'attachments.media_keys,author_id,geo.place_id',
        'media.fields': 'duration_ms,media_key,url,type,public_metrics',
        'user.fields': 'name,username',
        }
    pharse = {"value": f"{hashtag}"}
    return query_params, pharse

import requests
import time
from datetime import datetime
from datetime import timedelta


search_url = "https://api.twitter.com/2/tweets/search/all"
sleep_seconds = 300  # Sleep time in case of reach API limit


def create_headers(bearer_token):
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers


def query(headers, params, pagination_token, loop_counter):
    if loop_counter == 1:
        print(f"Loop {loop_counter} --> making API REQUEST")
        api_response = requests.request("GET", search_url, headers=headers, params=params, timeout=10)
        return api_response

    if loop_counter != 1:
        print(f"Loop {loop_counter} --> making API REQUEST")
        params["next_token"] = pagination_token
        api_response = requests.request("GET", search_url, headers=headers, params=params, timeout=10)
        return api_response


def query_controller(headers, params, pagination_token, loop_counter):
    try:
        api_response = query(headers, params, pagination_token, loop_counter)

        # controlling the API response

        if api_response.status_code == 200:
            print(f"Loop {loop_counter} --> API response OK")
            return api_response

        # API RATE LIMITS
        if api_response.status_code != 200:
            print(f"Loop {loop_counter} --> API RESPONSE FAIL STATUS RESPONSE: {api_response.status_code}")

            # IF API LIMIT REACHED

            if api_response.status_code == 429:

                actual_time = datetime.now()
                capture_time = actual_time.strftime("%d/%m/%Y %H:%M:%S")
                sleeping_time = timedelta(seconds=sleep_seconds)
                retry_time = actual_time + sleeping_time

                print(
                    f"Loop {loop_counter} --> API LIMIT REACHED at {capture_time}. RETRY AT {retry_time.strftime('%d/%m/%Y %H:%M:%S')}")
                time.sleep(sleep_seconds)
                print(f"Loop {loop_counter} --> Retry request {headers} {params}")
                return query_controller(headers, params, pagination_token, loop_counter)  # Recursion in request

            # error...

            else:
                raise Exception(api_response.status_code, api_response.text)

    # timeout...

    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        print(f"Loop {loop_counter} --> TIMEOUT! On {params} at {datetime.now()} Trying a RETRY ")
        return query_controller(headers, params, pagination_token, loop_counter)


import os
import json
import sys

from datetime import datetime


actual_time = datetime.now()

def save_data(json_response, loop_counter, pharse, filename, output_directory):
    output_folder = f"{output_directory}/api_responses/"
    json_parsed = json_response.json()
    if json_parsed["meta"]["result_count"] == 0:
        print("no tweets")
        n_tweets = 0
        last_date = "no tweets"
        return n_tweets, last_date
    else:
        data = json_parsed["data"]

        n_tweets = len(data)
        last = data[-1]
        last_date = last["created_at"]

        counter_n = str(loop_counter)

        # TESTING

        try:
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
            with open((output_folder + f"{filename}__loop-{counter_n}.json"), 'w', encoding='utf-8') as f:
                json.dump(json_parsed, f, ensure_ascii=False, indent=4)
                print(f"Loop {loop_counter} --> Dumped to JSON FILE {filename}")
            return n_tweets, last_date
        except IndexError:
            print("ERROR")
            pass


import glob
import pandas as pd
import json
from datetime import datetime
from tqdm import tqdm

output_folder = "output/"
actual_time = datetime.now()

global_frame = []


def extractor(file, works, position, hashtag):
    print(f"working on file {position + 1} // {works}")
    with open(file, encoding="utf-8") as jsonfile:
        data = json.load(jsonfile)

        if data["meta"]["result_count"] == 0:
            print("No results")
            return "empty"
        else:
            tweets = data["data"]

            general_df = []

            for element in tweets:
                try:
                    # Get the author name
                    list_of_users = data["includes"]["users"]
                    author_id = element["author_id"]
                    for user in list_of_users:
                        if user["id"] == author_id:
                            username = user["username"]
                            name = user["name"]
                        else:
                            pass

                    # In Reply To ID

                    reply_to_name = "false"
                    try:
                        reply_to_id = element["in_reply_to_user_id"]
                        entities = element["entities"]
                        mentions = entities["mentions"]
                        for mention in mentions:
                            if reply_to_id == mention["id"]:
                                reply_to_name = mention["username"]
                            else:
                                pass
                    except KeyError:
                        reply_to_id = "false"
                        reply_to_name = "false"

                    # Get entities

                    # Hashtag list
                    try:
                        entities = element["entities"]
                        hastags = entities["hashtags"]
                        hashtag_list = []
                        for hastag in hastags:
                            hash = hastag["tag"]
                            hashtag_list.append(hash)
                            hashtags_string = ";".join(hashtag_list)
                    except KeyError:
                        hashtags_string = "false"

                    # MENTIONS

                    try:
                        entities = element["entities"]
                        mentions = entities["mentions"]
                        mentions_list = []
                        for mention in mentions:
                            user = mention["username"]
                            mentions_list.append(user)
                            mentions_string = ";".join(mentions_list)
                    except KeyError:
                        mentions_string = "false"

                    mentions_string = mentions_string.split(';')
                    if len(mentions_string) < 2 or mentions_string is None:
                        # Generate the Dataframe
                        df = pd.DataFrame({
                            "tweet_id": element["id"],
                            "tweet_created_at": element["created_at"],
                            "user_id": element["author_id"],
                            "name": name,
                            "username": username,
                            "text": element["text"],
                            "rt_count": element["public_metrics"]["retweet_count"],
                            "like_count": element["public_metrics"]["like_count"],
                            "hashtags": hashtags_string,
                            "mentions": mentions_string
                        }, index=[0])
                        general_df.append(df)
                    else:
                        for i in range(0, len(mentions_string)):
                            df = pd.DataFrame({
                                "tweet_id": element["id"],
                                "tweet_created_at": element["created_at"],
                                "user_id": element["author_id"],
                                "name": name,
                                "username": username,
                                "text": element["text"],
                                "rt_count": element["public_metrics"]["retweet_count"],
                                "like_count": element["public_metrics"]["like_count"],
                                "hashtags": hashtags_string,
                                "mentions": mentions_string[i]
                            }, index=[0])
                            general_df.append(df)


                except(KeyError, IndexError):
                    pass

            final_df = pd.concat(general_df)

            return final_df



def crontroller(filename, hashtag, output_directory):
    files = glob.glob(f"{output_directory}/api_responses/{filename}*.json")
    print(files)
    works = len(files)
    for file in tqdm(files, desc="Pharsing JSON files..."):
        position = files.index(file)
        dataframe = extractor(file, works, position, hashtag)
        global_frame.append(dataframe)
    print("Creating df...")
    try:
        export_frame = pd.concat(global_frame)
        print("exporting df")
        export_frame.to_csv(f"{output_directory}/dataset-{filename}.csv", index=False, sep=",", quotechar='"',
                            line_terminator="\n")
        print("Done!")
        global_frame.clear()
    except (ValueError, TypeError):
        print("Nothing to Export")
        pass


import time
import os
import pandas as pd
from datetime import datetime

loop_counter = 1
sleeper = 6  # Controlling Rate limit API
total_tweets = 0
maximum_tweets = 10000000 # 10 Milion set is the maximum API QUOTA

### RECURSION LIMIT ####

import sys
sys.setrecursionlimit(round(maximum_tweets/450))
print(sys.getrecursionlimit())

### END RECURSION LIMIT ###

def loop(headers, query_params, pagination_token, loop_counter, filename, total_tweets):
    json_response = query_controller(headers, query_params, pagination_token, loop_counter)
    n_tweets, last_date = save_data(json_response, loop_counter, pharse, filename, output_directory)
    total_tweets = n_tweets + total_tweets
    actual_time = datetime.now()
    print(f"Loop {loop_counter} --> {query_params['query']} from {query_params['start_time']} to {query_params['end_time']} dumped to db at {actual_time}")
    print(f"Loop {loop_counter} --> Total Tweets downladed: " + str(total_tweets)+f" | Last date: {last_date}")
    try:
        if json_response.json()["meta"]["next_token"]:
            pagination_token = json_response.json()["meta"]["next_token"]
            time.sleep(sleeper)
            loop_counter += 1
            loop(headers, query_params, pagination_token, loop_counter, filename, total_tweets)
    except KeyError:
        print("Last Page")


def main(loop_counter, query_params, filename, total_tweets):
    headers = create_headers(bearer_token)
    pagination_token = None
    json_response = query_controller(headers, query_params, pagination_token, loop_counter)
    n_tweets, last_date = save_data(json_response, loop_counter, pharse, filename, output_directory)
    total_tweets = n_tweets + total_tweets

    actual_time = datetime.now()
    print(f"Loop {loop_counter} --> Query {loop_counter} | {query_params['query']} from {query_params['start_time']} to {query_params['end_time']} dumped to db at {actual_time}")
    print(f"Loop {loop_counter} --> Total Tweets downladed: " + str(total_tweets)+f" | Last date: {last_date}")

    try:
        if json_response.json()["meta"]["next_token"]:
            pagination_token = json_response.json()["meta"]["next_token"]
            loop_counter += 1
            time.sleep(sleeper)
            loop(headers, query_params, pagination_token, loop_counter, filename, total_tweets)
    except KeyError:
        print(f"Last Page for {query_params}")


if __name__ == "__main__":
    # Compose the query parameters
    start_date = datetime.strptime(start_date, "%d/%m/%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%d/%m/%Y").strftime("%Y-%m-%d")

    start_time = start_time
    end_time = end_time
    hashtag = hashtag
    output_directory = output_directory

    # create the query string
    query_params, pharse = parameters(start_date, end_date, start_time, end_time, hashtag)
    pharse = pharse["value"]
    query_params["query"] = pharse

    # Create the output name
    try:
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
    except IndexError:
        print("ERROR")
        pass

    # Generate the Filename
    actual_time = datetime.now()
    capture_time = actual_time.strftime("%d-%m-%Y-%H-%M-%S")
    filename = (f"{start_date}__to__{end_date}").replace(":", "-")

    # Start the extraction
    main(loop_counter, query_params, filename, total_tweets)

    # Sleeping 10 second between jobs to avoid reach API limits
    print("sleeping 10 secs between jobs")

    # CREATING DATAFRAMES

    crontroller(filename, hashtag, output_directory)
    print("Sleeping 10 Seconds")
    time.sleep(10)