# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from top2vec import Top2Vec
from sklearn.datasets import fetch_20newsgroups
import sys
import pickle


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    #newsgroups = fetch_20newsgroups(subset='all', remove=('headers', 'footers', 'quotes'))
    with open(sys.argv[2], 'rb') as handle:
        data = pickle.load(handle)

    model = Top2Vec(documents=data, embedding_model='universal-sentence-encoder', embedding_model_path=sys.argv[3])


    model.save(sys.argv[1])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
