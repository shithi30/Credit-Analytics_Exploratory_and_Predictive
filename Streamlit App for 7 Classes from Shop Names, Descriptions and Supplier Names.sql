-- sample OP: https://docs.google.com/spreadsheets/d/1sL504pikdfsMAy04MgbgdIBSFYJZbbUBKjhwfF7ajl4/edit#gid=0

/* scripts of models used */

-- Prediction of Business Types from Shop Names Using DNN, RNN, LSTM, Word2Vec.txt
-- Prediction of Business Types from Description and Supplier Names.txt

/* dataset preparation */ 

-- shop names
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	mobile mobile_no, 
	max(lower(regexp_replace(coalesce(shop_name, business_name, name, merchant_name), '[[:punct:]]', ' ', 'g'))) shop_name
from tallykhata.tallykhata_user_personal_info
where business_type in('GROCERY', 'PHARMACY', 'ELECTRONICS', 'MFS_MOBILE_RECHARGE', 'CLOTH_STORE', 'STATIONERY', 'HARDWARE')
group by 1; 

-- supplier names
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	mobile_no, 
	string_agg(
		distinct trim(
			regexp_replace(
				regexp_replace(
					translate(lower(name), '০১২৩৪৫৬৭৮৯', '0123456789'
					), '[^[:alpha:]]', ' ', 'g'
				), 
			'\s+', ' ', 'g'
			)
		), ' '
	) clean_supp_name
from public.account
where 
	is_active is true 
	and type=3 
group by 1; 
	
-- journal descriptions
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select  
	mobile_no, 
	string_agg(
		distinct trim(
			regexp_replace(
				regexp_replace(
					translate(description, '০১২৩৪৫৬৭৮৯', '0123456789'
					), '[^[:alpha:]]', ' ', 'g'
				), 
			'\s+', ' ', 'g'
			)
		), ' '
	) clean_description  
from public.journal
where 
	description !~* '[a-z]' 
	and description is not null 
	and description!=''
	and txn_type in(1, 2, 3, 4, 11)
	and date(create_date)>=current_date-7 and date(create_date)<current_date
group by 1; 

-- data to feed in (also in St. app)
select * 
from 
	(select 
		mobile_no, 
		shop_name, 
		case when clean_supp_name is null then 'unavailable' else clean_supp_name end clean_supp_name, 
		case when clean_description is null then 'unavailable' else clean_description end clean_description 
	from 
		data_vajapora.help_a tbl1 
		inner join 
		data_vajapora.help_b tbl2 using(mobile_no)
		inner join 
		data_vajapora.help_c tbl3 using(mobile_no) 
	-- for complete cases
	where 
		clean_supp_name!=''
		and clean_description!=''
	order by random() 
	-- flexible
	limit 500
	) tbl1 
	
	inner join 
	
	(select 
		mobile mobile_no, 
		max(replace(lower(new_bi_business_type), '_', '')) rule_classes,
		max(replace(lower(business_type), '_', '')) given_classes
	from tallykhata.tallykhata_user_personal_info
	where business_type in('GROCERY', 'PHARMACY', 'ELECTRONICS', 'MFS_MOBILE_RECHARGE', 'CLOTH_STORE', 'STATIONERY', 'HARDWARE')
	group by 1
	) tbl2 using(mobile_no); 

-- input file for unseen data
select mobile "mobile_no","shop_name", '' "clean_supp_name", '' "clean_description", '' "rule_classes", '' "given_classes"
from tallykhata.tallykhata_user_personal_info 
where 1=1
	and (business_type is null or business_type='')
	and shop_name is not null
order by random()
limit 500;
	
/* Streamlit app code */

-- path: C:\Users\progoti\PycharmProjects\pythonProject\venv\app2.py

import streamlit as st
import io
import csv
from PIL import Image
import copy
import numpy as np
import pandas as pd
import math
from statistics import mode
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.models import load_model
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import Error
import datetime
import time

st.set_page_config(
    page_title="Biz Type Prediction",
    page_icon=Image.open(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\icon.png"),
    layout='centered',
    initial_sidebar_state='expanded',
    menu_items={
        'About': "This app is developed at PSL, by Shithi Maitra. Contact: shithi.maitra@surecash.net"
    }
)
st.title("Predicting Biz-Types from Suppilers, Names and Transaction Descriptions")

def parse_data_from_file(filename):
    sentences = []
    labels = []
    with open(filename, 'r', encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)
        for row in reader:
            labels.append(row[0])
            sentence = row[1]
            sentences.append(sentence)
    return sentences, labels

def train_val_split(sentences, labels, training_split):
    # Compute the number of sentences that will be used for training (should be an integer)
    train_size = round(len(sentences)*training_split)
    # Split the sentences and labels into train/validation splits
    train_sentences = sentences[0:train_size]
    train_labels = labels[0:train_size]
    validation_sentences = sentences[train_size:]
    validation_labels = labels[train_size:]
    return train_sentences, validation_sentences, train_labels, validation_labels

def fit_tokenizer(train_sentences, num_words, oov_token):
    # Instantiate the Tokenizer class, passing in the correct values for num_words and oov_token
    tokenizer = Tokenizer(num_words = num_words, oov_token=oov_token)
    # Fit the tokenizer to the training sentences
    tokenizer.fit_on_texts(train_sentences)
    return tokenizer

def seq_and_pad(sentences, tokenizer, padding, maxlen):
    # Convert sentences to sequences
    sequences = tokenizer.texts_to_sequences(sentences)
    # Pad the sequences using the correct padding and maxlen
    padded_sequences = pad_sequences(sequences, maxlen=maxlen, padding=padding)
    return padded_sequences

def tokenize_labels(all_labels, split_labels):
    # Instantiate the Tokenizer (no additional arguments needed)
    label_tokenizer = Tokenizer()
    # Fit the tokenizer on all the labels
    label_tokenizer.fit_on_texts(all_labels)
    # Convert labels to sequences
    label_seq = label_tokenizer.texts_to_sequences(split_labels)
    # Convert sequences to a numpy array. Don't forget to substact 1 from every entry in the array!
    label_seq_np = np.array(label_seq)-1
    return label_seq_np

def categories_into_numerics(all_labels):
    # Instantiate the Tokenizer (no additional arguments needed)
    label_tokenizer = Tokenizer()
    # Fit the tokenizer on all the labels
    label_tokenizer.fit_on_texts(all_labels)
    # Get categories into numerics
    categories = list(label_tokenizer.word_index)
    return categories

# From supplier names

NUM_WORDS = 500000
EMBEDDING_DIM = 16
suppname_MAXLEN = 20
suppname_PADDING = 'post'
OOV_TOKEN = "<OOV>"
TRAINING_SPLIT = .85

sentences, labels = parse_data_from_file(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\type_pred_supp_name.csv")
train_sentences, val_sentences, train_labels, val_labels = train_val_split(sentences, labels, TRAINING_SPLIT)
suppname_tokenizer = fit_tokenizer(train_sentences, NUM_WORDS, OOV_TOKEN)
train_padded_seq = seq_and_pad(train_sentences, suppname_tokenizer, suppname_PADDING, suppname_MAXLEN)
val_padded_seq = seq_and_pad(val_sentences, suppname_tokenizer, suppname_PADDING, suppname_MAXLEN)
train_label_seq = tokenize_labels(labels, train_labels)
val_label_seq = tokenize_labels(labels, val_labels)
suppname_categories = categories_into_numerics(labels)

# From descriptions

NUM_WORDS = 500000
EMBEDDING_DIM = 16
desc_MAXLEN = 20
desc_PADDING = 'post'
OOV_TOKEN = "<OOV>"
TRAINING_SPLIT = .85

sentences, labels = parse_data_from_file(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\type_pred_desc_data.csv")
train_sentences, val_sentences, train_labels, val_labels = train_val_split(sentences, labels, TRAINING_SPLIT)
desc_tokenizer = fit_tokenizer(train_sentences, NUM_WORDS, OOV_TOKEN)
train_padded_seq = seq_and_pad(train_sentences, desc_tokenizer, desc_PADDING, desc_MAXLEN)
val_padded_seq = seq_and_pad(val_sentences, desc_tokenizer, desc_PADDING, desc_MAXLEN)
train_label_seq = tokenize_labels(labels, train_labels)
val_label_seq = tokenize_labels(labels, val_labels)
desc_categories = categories_into_numerics(labels)

# From shop names

NUM_WORDS = 500000
EMBEDDING_DIM = 16
shopname_MAXLEN = 5
shopname_PADDING = 'post'
OOV_TOKEN = "<OOV>"
TRAINING_SPLIT = .85

sentences, labels = parse_data_from_file(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\type_pred_02.csv")
train_sentences, val_sentences, train_labels, val_labels = train_val_split(sentences, labels, TRAINING_SPLIT)
shopname_tokenizer = fit_tokenizer(train_sentences, NUM_WORDS, OOV_TOKEN)
train_padded_seq = seq_and_pad(train_sentences, shopname_tokenizer, shopname_PADDING, shopname_MAXLEN)
val_padded_seq = seq_and_pad(val_sentences, shopname_tokenizer, shopname_PADDING, shopname_MAXLEN)
train_label_seq = tokenize_labels(labels, train_labels)
val_label_seq = tokenize_labels(labels, val_labels)
shopname_categories = categories_into_numerics(labels)

# Get models
model  = load_model(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\suppname_model.h5")
model2 = load_model(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\desc_model.h5")
model3 = load_model(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\shopname_model.h5")

# Get times
if 'elapsed_time' not in st.session_state: st.session_state.elapsed_time=0
if_cache_hit = True

# Get dataframe from database
@st.cache(suppress_st_warning=True, show_spinner=False)
def get_db_df(ex):
    try:
        # Connect to DB
        connection = psycopg2.connect(user="shithi", password="tallykhata03012021_1234", host="10.192.192.52", port="5432", database="tallykhata")
        cursor = connection.cursor()
        # Extract data
        postgres_query = '''
        select * 
        from 
            (select 
                mobile_no, 
                shop_name, 
                case when clean_supp_name is null then 'unavailable' else clean_supp_name end clean_supp_name, 
                case when clean_description is null then 'unavailable' else clean_description end clean_description 
            from 
                data_vajapora.help_a tbl1 
                inner join 
                data_vajapora.help_b tbl2 using(mobile_no)
                inner join 
                data_vajapora.help_c tbl3 using(mobile_no) 
            where clean_description!=''
            order by random() 
            limit '''+ex+'''
            ) tbl1 
            
            inner join 
            
            (select 
                mobile mobile_no, 
                max(replace(lower(new_bi_business_type), '_', '')) rule_classes,
                max(replace(lower(business_type), '_', '')) given_classes
            from tallykhata.tallykhata_user_personal_info
            where business_type in('GROCERY', 'PHARMACY', 'ELECTRONICS', 'MFS_MOBILE_RECHARGE', 'CLOTH_STORE', 'STATIONERY', 'HARDWARE')
            group by 1
            ) tbl2 using(mobile_no);
        '''
        cursor.execute(postgres_query)
        # Write the dataframe
        colnames = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=colnames)
    # Handle exception
    except (Exception, psycopg2.Error) as error: print("Error!", error)
    # Close connection to DB
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("\nPostgreSQL connection is safely closed.")
    return df

# Get predictions
@st.cache(suppress_st_warning=True, show_spinner=False)
def get_predictions(df):

    input_shops = [str(item) for item in list(df["shop_name"])]
    input_desc = [str(item) for item in list(df["clean_description"])]
    input_suppliers = [str(item) for item in list(df["clean_supp_name"])]

    output_classes_priority = []
    output_probs = []
    model_acting = []
    output_classes_mod = []
    output_classes_name = []
    output_classes_desc = []
    output_classes_supp = []

    l = len(input_shops)
    start_time = time.time()
    progress = st.progress(0.0)
    for i in range(0, l):
        # Pipelined models
        # Shop names
        pred_probabilities = model3.predict(seq_and_pad([input_shops[i]], shopname_tokenizer, shopname_PADDING, shopname_MAXLEN))
        if (pred_probabilities[0, np.argmax(pred_probabilities)] < 0.7 or input_shops[i] == "unavailable"):
            # Descriptions
            pred_probabilities = model2.predict(seq_and_pad([input_desc[i]], desc_tokenizer, desc_PADDING, desc_MAXLEN))
            if (pred_probabilities[0, np.argmax(pred_probabilities)] < 0.7 or input_desc[i] == "unavailable"):
                # Supplier names
                pred_probabilities = model.predict(seq_and_pad([input_suppliers[i]], suppname_tokenizer, suppname_PADDING, suppname_MAXLEN))
                if (pred_probabilities[0, np.argmax(pred_probabilities)] < 0.7 or input_suppliers[i] == "unavailable"):
                    pred_class = "unidentified"
                    output_classes_priority.append(pred_class)
                    pred_probability = 0
                    output_probs.append(pred_probability)
                    model_acting.append("none")
                else:
                    pred_class = suppname_categories[np.argmax(pred_probabilities)]
                    output_classes_priority.append(pred_class)
                    pred_probability = pred_probabilities[0, np.argmax(pred_probabilities)]
                    output_probs.append(pred_probability)
                    model_acting.append("suppname")
            else:
                pred_class = desc_categories[np.argmax(pred_probabilities)]
                output_classes_priority.append(pred_class)
                pred_probability = pred_probabilities[0, np.argmax(pred_probabilities)]
                output_probs.append(pred_probability)
                model_acting.append("desc")
        else:
            pred_class = shopname_categories[np.argmax(pred_probabilities)]
            output_classes_priority.append(pred_class)
            pred_probability = pred_probabilities[0, np.argmax(pred_probabilities)]
            output_probs.append(pred_probability)
            model_acting.append("shopname")

        # Mode models
        ls_mode = []
        # Shop names
        if input_shops[i] != "unavailable":
            pred_probabilities = model3.predict(seq_and_pad([input_shops[i]], shopname_tokenizer, shopname_PADDING, shopname_MAXLEN))
            pred_class = shopname_categories[np.argmax(pred_probabilities)]
            ls_mode.append(pred_class)
            output_classes_name.append(pred_class)
        else: output_classes_name.append("unidentified")
        # Descriptions
        if input_desc[i] != "unavailable":
            pred_probabilities = model2.predict(seq_and_pad([input_desc[i]], desc_tokenizer, desc_PADDING, desc_MAXLEN))
            pred_class = desc_categories[np.argmax(pred_probabilities)]
            ls_mode.append(pred_class)
            output_classes_desc.append(pred_class)
        else: output_classes_desc.append("unidentified")
        # Supplier names
        if input_suppliers[i] != "unavailable":
            pred_probabilities = model.predict(seq_and_pad([input_suppliers[i]], suppname_tokenizer, suppname_PADDING, suppname_MAXLEN))
            pred_class = suppname_categories[np.argmax(pred_probabilities)]
            ls_mode.append(pred_class)
            output_classes_supp.append(pred_class)
        else: output_classes_supp.append("unidentified")

        try: output_classes_mod.append(mode(ls_mode))
        except: output_classes_mod.append("unidentified")
        progress.progress(i*1/(l-1))

    df["output_classes_priority"] = output_classes_priority
    df["output_probs"] = output_probs
    df["model_acting"] = model_acting
    df["output_classes_mod"] = output_classes_mod
    df["output_classes_name"] = output_classes_name
    df["output_classes_desc"] = output_classes_desc
    df["output_classes_supp"] = output_classes_supp

    st.session_state.elapsed_time = round((time.time()-start_time)/60.00, 2)
    global if_cache_hit
    if_cache_hit = False

    return df

# Get comparative performance
def comparative_performance(df):

    labels = ['correct', 'incorrect', 'unidentified']
    colors = ['#C43A1C','#F8D80B','#CBBCB8']

    numerals_output_classes_priority = [0, 0, 0]
    numerals_output_classes_mod = [0, 0, 0]
    numerals_rule_classes = [0, 0, 0]
    numerals_output_classes_name = [0, 0, 0]
    numerals_output_classes_desc = [0, 0, 0]
    numerals_output_classes_supp = [0, 0, 0]

    given_classes = [str(item) for item in list(df['given_classes'])]
    output_classes_priority = [str(item) for item in list(df['output_classes_priority'])]
    output_classes_mod = [str(item) for item in list(df['output_classes_mod'])]
    rule_classes = [str(item) for item in list(df['rule_classes'])]
    output_classes_name = [str(item) for item in list(df['output_classes_name'])]
    output_classes_desc = [str(item) for item in list(df['output_classes_desc'])]
    output_classes_supp = [str(item) for item in list(df['output_classes_supp'])]

    l = len(given_classes)
    for i in range(0, l):
        # output_classes_priority
        if output_classes_priority[i]=='unidentified': numerals_output_classes_priority[2]=numerals_output_classes_priority[2]+1
        elif output_classes_priority[i]!=given_classes[i]: numerals_output_classes_priority[1]=numerals_output_classes_priority[1]+1
        else: numerals_output_classes_priority[0]=numerals_output_classes_priority[0]+1
        # output_classes_mod
        if output_classes_mod[i]=='unidentified': numerals_output_classes_mod[2]=numerals_output_classes_mod[2]+1
        elif output_classes_mod[i]!=given_classes[i]: numerals_output_classes_mod[1]=numerals_output_classes_mod[1]+1
        else: numerals_output_classes_mod[0]=numerals_output_classes_mod[0]+1
        # rule_classes
        if rule_classes[i][0:3]=='oth': numerals_rule_classes[2]=numerals_rule_classes[2]+1
        elif rule_classes[i][0:3]!=given_classes[i][0:3]: numerals_rule_classes[1]=numerals_rule_classes[1]+1
        else: numerals_rule_classes[0]=numerals_rule_classes[0]+1
        # output_classes_name
        if output_classes_name[i]=='unidentified': numerals_output_classes_name[2]=numerals_output_classes_name[2]+1
        elif output_classes_name[i]!=given_classes[i]: numerals_output_classes_name[1]=numerals_output_classes_name[1]+1
        else: numerals_output_classes_name[0]=numerals_output_classes_name[0]+1
        # output_classes_desc
        if output_classes_desc[i]=='unidentified': numerals_output_classes_desc[2]=numerals_output_classes_desc[2]+1
        elif output_classes_desc[i]!=given_classes[i]:numerals_output_classes_desc[1]=numerals_output_classes_desc[1]+1
        else: numerals_output_classes_desc[0]=numerals_output_classes_desc[0]+1
        # output_classes_supp
        if output_classes_supp[i]=='unidentified': numerals_output_classes_supp[2]=numerals_output_classes_supp[2]+1
        elif output_classes_supp[i]!=given_classes[i]: numerals_output_classes_supp[1]=numerals_output_classes_supp[1]+1
        else: numerals_output_classes_supp[0]=numerals_output_classes_supp[0]+1

    # # Draw charts (v1)
    # explode = (0.125, 0, 0)
    # plt.rcParams.update({'font.size': 28})
    # fig, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=True, constrained_layout = True, figsize=(25, 25))
    # ax1.pie(numerals_output_classes_priority, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    # ax1.set_title('Prioritized Model')
    # ax1.legend(labels, loc="lower left")
    # ax2.pie(numerals_output_classes_mod, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    # ax2.set_title('Mode Model')
    # ax2.legend(labels, loc="lower left")
    # ax3.pie(numerals_rule_classes, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    # ax3.set_title('Rules-based Model')
    # ax3.legend(labels, loc="lower left")
    # st.pyplot(fig)

    # Draw charts
    explode = (0.08, 0.08, 0.08)
    plt.rcParams.update({'font.size': 13})
    fig, a = plt.subplots(2, 3, sharey=True, sharex=True, constrained_layout=True, figsize=(15, 15))
    a[0][0].pie(numerals_output_classes_priority, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    a[0][0].set_title("Prioritized Model")
    a[0][0].legend(labels, loc='lower right')
    a[0][1].pie(numerals_output_classes_mod, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    a[0][1].set_title("Mode Model")
    a[0][1].legend(labels, loc='lower right')
    a[0][2].pie(numerals_rule_classes, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    a[0][2].set_title("Rules-based Model")
    a[0][2].legend(labels, loc='lower right')
    a[1][0].pie(numerals_output_classes_name, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    a[1][0].set_title('Shop Name Model')
    a[1][0].legend(labels, loc="lower right")
    a[1][1].pie(numerals_output_classes_desc, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    a[1][1].set_title('Description Model')
    a[1][1].legend(labels, loc="lower right")
    a[1][2].pie(numerals_output_classes_supp, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90, colors=colors)
    a[1][2].set_title('Supplier Name Model')
    a[1][2].legend(labels, loc="lower right")
    fig.subplots_adjust(left=None, bottom=0.4, right=None, top=None, wspace=None, hspace=None)
    st.pyplot(fig)

    acc = [numerals_output_classes_priority[0], numerals_output_classes_mod[0], numerals_rule_classes[0],
           numerals_output_classes_name[0], numerals_output_classes_desc[0], numerals_output_classes_supp[0]]
    avg_acc = round(np.mean(acc, 0) / l * 100, 2)
    avg_up = round(np.max(acc, 0) / l * 100 - avg_acc, 2)

    err = [numerals_output_classes_priority[1], numerals_output_classes_mod[1], numerals_rule_classes[1],
           numerals_output_classes_name[1], numerals_output_classes_desc[1], numerals_output_classes_supp[1]]
    avg_err = round(np.mean(err, 0) / l * 100, 2)
    avg_down = round(np.min(err, 0) / l * 100 - avg_err, 2)

    dict_metrics = {
        'avg_acc': avg_acc,
        'avg_up': avg_up,
        'avg_err': avg_err,
        'avg_down': avg_down
    }

    return dict_metrics

st.sidebar.title("Welcome!")
st.sidebar.subheader("Use this pane for better experience.")
st.sidebar.image(Image.open(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\pane.png"), width=258)
rad=st.sidebar.radio("Set Input Modality:", ["User Input", "Upload File as Input", "Input from Database"])

if rad=="User Input":

    mod = st.sidebar.multiselect("Select Modality of Prediction (for User Input):", ["All", "Shop Names", "Sale/Purchase Descriptions", "Supplier Names"])
    if len(mod)==0: mod="All"

    # From supplier names
    if ("All" in mod or "Supplier Names" in mod):
        nm = st.text_input("Enter one of your suppliers' names (Bengali/English):")
        if len(nm)>0:
            pred_probabilities = model.predict(seq_and_pad([nm], suppname_tokenizer, suppname_PADDING, suppname_MAXLEN))
            pred_class = suppname_categories[np.argmax(pred_probabilities)]
            st.write("Your predicted business type is: "+"**"+str(pred_class)+"**")
            pred_probability = pred_probabilities[0, np.argmax(pred_probabilities)]
            st.write("Probability of this result's being true is: "+"**"+str(pred_probability)+"**")

    # From txn descriptions
    if ("All" in mod or "Sale/Purchase Descriptions" in mod):
        descr = st.text_input("Enter any description of transaction (only Bengali):")
        if len(descr)>0:
            pred_probabilities = model2.predict(seq_and_pad([descr], desc_tokenizer, desc_PADDING, desc_MAXLEN))
            pred_class = desc_categories[np.argmax(pred_probabilities)]
            st.write("Your predicted business type is: "+"**"+str(pred_class)+"**")
            pred_probability = pred_probabilities[0, np.argmax(pred_probabilities)]
            st.write("Probability of this result's being true is: "+"**"+str(pred_probability)+"**")

    # From shop names
    if ("All" in mod or "Shop Names" in mod):
        shnm = st.text_input("Enter the name of your shop (Bengali/English):")
        if len(shnm)>0:
            pred_probabilities = model3.predict(seq_and_pad([shnm], shopname_tokenizer, shopname_PADDING, shopname_MAXLEN))
            pred_class = shopname_categories[np.argmax(pred_probabilities)]
            st.write("Your predicted business type is: "+"**"+str(pred_class)+"**")
            pred_probability = pred_probabilities[0, np.argmax(pred_probabilities)]
            st.write("Probability of this result's being true is: "+"**"+str(pred_probability)+"**")

if rad == "Upload File as Input":

    input_file = st.file_uploader("Upload a file of shop names, descriptions and supplier names.")
    if input_file is not None:
        # Get data
        file_df = pd.read_csv(input_file)
        with st.spinner("Uploading ..."): time.sleep(1)
        st.info("Your file has been uploaded. We are generating predictions, please wait.")
        # Get results
        file_df = get_predictions(file_df)
        # Show performance
        if if_cache_hit is False: st.success("Predictions generated successfully. Elapsed time (minutes): " + str(st.session_state.elapsed_time))
        else: st.success("Predictions generated previously, retrieved results from cache. Hence, elapsed time (minutes): 0")
        # Show results
        st.warning("Inspect your prediction quality. Download if satisfied.")
        st.dataframe(file_df, width=700)
        if if_cache_hit is False: st.balloons()
        # Download results
        dwd = st.download_button(label="Download predictions (.csv)", data=file_df.to_csv(index=False).encode('utf-8'), file_name="ML-OP-UP.csv", mime='text/csv')
        if dwd is True: st.success("Download successful, thanks for using the app!")

if rad == "Input from Database":

    ex = st.text_input("How many examples from database would you like to work on?")
    if len(ex) > 0:
        # Get data
        with st.spinner("Loading from DB ..."): file_df2 = copy.deepcopy(get_db_df(ex))
        # Get results
        file_df2 = get_predictions(file_df2)
        # Show performance
        col3, col2, col1 = st.columns([1, 1, 3.8])
        with st.spinner("Loading results ..."): dict_metrics = comparative_performance(file_df2)
        if if_cache_hit is False: col1.success("Predictions generated successfully.  \nElapsed time (minutes): " + str(st.session_state.elapsed_time))
        else: col1.info("Predictions generated previously, retrieved results from cache.  \nHence, elapsed time (minutes): 0")
        col2.metric(label="Avg. Error", value=str(dict_metrics['avg_err']) + "%", delta=str(dict_metrics['avg_down']) + "%")
        col3.metric(label="Avg. Accuracy", value=str(dict_metrics['avg_acc']) + "%", delta=str(dict_metrics['avg_up']) + "%")
        # Show results
        st.warning("Inspect your prerdiction quality. Download if satisfied.")
        st.dataframe(file_df2, width=700)
        # Download results
        dwd = st.download_button(label="Download predictions (.csv)", data=file_df2.to_csv(index=False).encode('utf-8'), file_name="ML-OP-UP.csv", mime='text/csv')
        if dwd is True: st.success("Download successful, thanks for using the app!")

st.image(Image.open(r"C:\Users\progoti\PycharmProjects\pythonProject\venv\Tallykhata-Logo-EN.png"), width=300)