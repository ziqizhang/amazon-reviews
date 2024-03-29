# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import traceback

from top2vec import Top2Vec
import sys, os, datetime, logging


logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("top2vec")

def train_top2vec_model(in_file, out_file, model_path, min_words=5, min_freq=20):
    file = open(in_file, 'r')
    lines=[]

    count_total=0
    count_selected=0
    for l in file:
        count_total+=1
        l=l.strip()
        if len(l.split(" "))<min_words:
            continue
        count_selected+=1
        lines.append(l)

    print(">>>\t\t\ttotal lines={}, selected={}".format(count_total, count_selected))
    try:
        model = Top2Vec(documents=lines, embedding_model='universal-sentence-encoder', embedding_model_path=model_path,
                    min_count=min_freq)
        model.save(out_file)
    except:
        print(traceback.format_exc())

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    in_folder=sys.argv[1]
    out_folder=sys.argv[2]
    model_path=sys.argv[3]

    files=os.listdir(in_folder)
    print(">>>\t\tTotal files={}".format(len(files)))
    files.sort()
    print(">>>\t\tBeginning the process")
    for file in files:
        print(">>>\t\t{} training for {}".format(datetime.datetime.now(), file))
        train_top2vec_model(in_folder+"/"+file, out_folder+"/topics_"+file, model_path)
        print(">>>\t\tcompleted")


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
