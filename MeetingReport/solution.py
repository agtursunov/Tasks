import pandas as pd
import json

def read_file(file_path):
    raw_data = pd.read_excel(file_path, sheet_name='Sheet1', header=0)

    raw_data.iloc[2, 2] = raw_data.iloc[2, 2][:-2]

    raw_data['raw_content_dict'] = raw_data['raw_content'].apply(json.loads)
    return raw_data

raw_data = read_file('raw_data.xlsx')

comm_types = raw_data['comm_type'].unique()
dim_comm_type = pd.DataFrame({'comm_type': comm_types, 'comm_type_id': range(1, len(comm_types) + 1)})

subjects = raw_data['subject']
dim_subject = pd.DataFrame({'subject': subjects, 'subject_id': range(101, len(subjects) + 101)})

calendar_ids = []
for raw_data_dic in raw_data['raw_content_dict']:
    calendar_ids.append(raw_data_dic["calendar_id"])
calendar_ids = pd.Series(calendar_ids)
dim_calendar = pd.DataFrame({'raw_calendar_id': calendar_ids, 'calendar_id': range(301, len(calendar_ids) + 301)})

audio_urls = []
for raw_data_dic in raw_data['raw_content_dict']:
    audio_urls.append(raw_data_dic["audio_url"])
audio_urls = pd.Series(audio_urls)
dim_audio = pd.DataFrame({'raw_audio_url': audio_urls, 'audio_id': range(1001, len(audio_urls) + 1001)})

video_urls = []
for raw_data_dic in raw_data['raw_content_dict']:
    video_urls.append(raw_data_dic["video_url"])
video_urls = pd.Series(video_urls)
dim_video = pd.DataFrame({'raw_video_url': video_urls, 'video_id': range(1, len(video_urls) + 1)})

transcript_urls = []
for raw_data_dic in raw_data['raw_content_dict']:
    transcript_urls.append(raw_data_dic["transcript_url"])
transcript_urls = pd.Series(transcript_urls)
dim_transcript = pd.DataFrame({'raw_transcript_url': transcript_urls, 'transcript_id': range(801, len(transcript_urls) + 801)})

dateStrings = []
for raw_data_dic in raw_data['raw_content_dict']:
    dateStrings.append(raw_data_dic["dateString"])
dateStrings = pd.Series(dateStrings)
dim_dateString= pd.DataFrame({'DateTime': dateStrings, 'datetime_id': range(91, len(dateStrings) + 91)})


meeting_attendees_df = pd.DataFrame()
speakers_df = pd.DataFrame()
participants_df = pd.DataFrame()
organizer_emails_df = pd.DataFrame()
host_emails_df = pd.DataFrame()
raw_content_param_df = pd.DataFrame()
for raw_data_dic in raw_data['raw_content_dict']:
    raw_content_param = pd.DataFrame({'email': [raw_data_dic["host_email"]]})
    raw_content_param['id'] = raw_data_dic["id"]
    raw_content_param['raw_title'] = raw_data_dic["title"]
    raw_content_param['raw_duration'] = raw_data_dic["duration"]
    organizer_emails = pd.DataFrame({'email': [raw_data_dic["organizer_email"]]})
    organizer_emails['comm_id'] = raw_data_dic['id']
    host_emails = pd.DataFrame({'email': [raw_data_dic["host_email"]]})
    host_emails['comm_id'] = raw_data_dic['id']
    speakers = pd.DataFrame(raw_data_dic["speakers"])
    speakers['comm_id'] = raw_data_dic['id']
    participants = pd.DataFrame({'email': raw_data_dic["participants"]}, columns=['email'])
    participants['comm_id'] = raw_data_dic['id']
    meeting_attendees = pd.DataFrame(raw_data_dic["meeting_attendees"])
    meeting_attendees['comm_id'] = raw_data_dic['id']
    raw_content_param_df = pd.concat([raw_content_param_df, raw_content_param], ignore_index=True)
    host_emails_df = pd.concat([host_emails_df, host_emails], ignore_index=True)
    organizer_emails_df = pd.concat([organizer_emails_df, organizer_emails], ignore_index=True)
    speakers_df = pd.concat([speakers_df, speakers], ignore_index=True)
    participants_df = pd.concat([participants_df, participants], ignore_index=True)
    meeting_attendees_df = pd.concat([meeting_attendees_df, meeting_attendees], ignore_index=True)

extended_users = pd.concat([meeting_attendees_df, organizer_emails_df, participants_df, speakers_df, host_emails_df], ignore_index=True)
dim_user = extended_users.drop_duplicates().drop(columns=['comm_id'])
dim_user.insert(0, 'user_id', range(1, len(dim_user) + 1))

fact_communication = pd.DataFrame()
fact_communication['comm_id'] = raw_content_param_df['id']
fact_communication['raw_id'] = raw_data['id']
fact_communication['source_id'] = raw_data['source_id']

fact_communication['comm_type'] = raw_data['comm_type']
fact_communication = fact_communication.merge(dim_comm_type, on='comm_type', how='left')
fact_communication.drop(columns=['comm_type'], inplace=True)

fact_communication['subject_id'] = dim_subject['subject_id']
fact_communication['calendar_id'] = dim_calendar['calendar_id']
fact_communication['audio_id'] = dim_audio['audio_id']
fact_communication['video_id'] = dim_video['video_id']
fact_communication['transcript_id'] = dim_transcript['transcript_id']
fact_communication['datetime_id'] = dim_dateString['datetime_id']
fact_communication['ingested_at'] = raw_data['ingested_at']
fact_communication['processed_at'] = raw_data['processed_at']
fact_communication['is_processed'] = raw_data['is_processed']
fact_communication['raw_title'] = raw_content_param_df['raw_title']
fact_communication['raw_duration'] = raw_content_param_df['raw_duration']


def add_role_flag(base_df, role_df, role_name):
    temp = base_df.merge(role_df, on=["email", "comm_id"], how="left", indicator=True)
    base_df[role_name] = temp["_merge"] == "both"
    return base_df

extended_users = add_role_flag(extended_users, meeting_attendees_df, "attended")
extended_users = add_role_flag(extended_users, organizer_emails_df, "is_organizer")
extended_users = add_role_flag(extended_users, participants_df, "is_participant")
temp = extended_users.merge(speakers_df, on=["name", "comm_id"], how="left", indicator=True)
extended_users['is_speaker'] = temp["_merge"] == "both"
extended_users.drop(columns=['location', 'displayName', 'phoneNumber'], inplace=True)
bridge_comm_user = pd.merge(extended_users, dim_user, on = ['name', 'email'], how='left')
bridge_comm_user.drop(columns=['name', 'email'], inplace=True)
user_id_column = bridge_comm_user.pop('user_id')
bridge_comm_user.insert(1, 'user_id', user_id_column)


def write_to_excel(sheets):
    with pd.ExcelWriter('results.xlsx') as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

def main():
    raw_date = read_file('raw_data.xlsx')
    sheets = {
        'dim_comm_type': dim_comm_type,
        'dim_subject': dim_subject,
        'dim_user': dim_user,
        'dim_calendar': dim_calendar,
        'dim_audio': dim_audio,
        'dim_video': dim_video,
        'dim_transcript': dim_transcript,
        'fact_communication': fact_communication,
        'bridge_comm_user': bridge_comm_user}
    write_to_excel(sheets)
    pass

if __name__ == "__main__":
    main()