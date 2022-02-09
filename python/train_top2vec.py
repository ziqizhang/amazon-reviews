# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from top2vec import Top2Vec
import sys, os, datetime, logging


logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("top2vec")

def train_top2vec_model(in_file, out_file, model_path):
    file = open(in_file, 'r')
    lines= file.readlines()
    model = Top2Vec(documents=lines, embedding_model='universal-sentence-encoder', embedding_model_path=model_path,
                    min_count=10)
    model.save(out_file)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    in_folder=sys.argv[1]
    out_folder=sys.argv[2]
    model_path=sys.argv[3]

    log.info(">>>\t\tBeginning the process")
    for file in os.listdir(in_folder):
        log.info(">>>\t\t{} training for {}".format(datetime.datetime.now(), file))
        train_top2vec_model(in_folder+"/"+file, out_folder+"/topics_"+file, model_path)
        log.info(">>>\t\tcompleted")


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
