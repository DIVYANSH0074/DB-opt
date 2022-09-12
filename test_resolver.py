import pandas as pd
import json
from reg_hub_spoke.adf_resolver.resolver import get_all_uids_from_adfs
from reg_hub_spoke.adf_resolver.resolver_optimized import get_uid_frames
import datetime


def frame_overall():
    with open('/Users/astro/Desktop/frame_USC6.json', 'r') as openfile:
        df = json.load(openfile)
    uid = df['UIDs']
    frames = df['frames']
    frame = []
    for f in frames:
        for inside in f:
            frame.append(inside)
    frames = []
    for index, f in enumerate(frame):
        frames.append(f)
        if index == 1000:
            break
    return frames


def old_resolver():
    references = {}
    frames = frame_overall()
    for frame in frames:
        ref_list = get_all_uids_from_adfs(frame.get('adfs'))
        references[frame["cites"][0]["cite_text"]] = ref_list
    return references


def new_resolver():
    references = {}
    frames = frame_overall()
    reference = get_uid_frames(frames)
    for ref in reference:
        references[ref["cites"][0]["cite_text"]] = ref['uid_fields'][0]['UID']
    return references


frames = frame_overall()
print(len(frames))

start_old = datetime.datetime.now()
old_ref = {}
old_ref = old_resolver()
end_old = datetime.datetime.now()
total_old = end_old - start_old

start_new = datetime.datetime.now()
new_ref = {}
new_ref = new_resolver()
end_new = datetime.datetime.now()
total_new = end_new - start_new

print("new resolver: {}".format(total_new))
print("old resolver: {}".format(total_old))

frames = frame_overall()
print("total frames: {}".format(len(frames)))

print("old len: {}".format(len(old_ref)))
print("new len: {}".format(len(new_ref)))

if old_ref == new_ref:
    print('true')
else:
    print('false')

with open("new_uid.json", "w") as newfile:
    json.dump(new_ref, newfile)

with open("old_uid.json", "w") as oldfile:
    json.dump(old_ref, oldfile)
