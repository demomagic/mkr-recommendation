import argparse
import numpy as np
from data_loader import load_data
from train import train

np.random.seed(555)

parser = argparse.ArgumentParser()

# movie
parser.add_argument('-dataset','--dataset', type=str, default='movie', help='which dataset to use')
parser.add_argument('-n_epochs','--n_epochs', type=int, default=20, help='the number of epochs')
parser.add_argument('-dim','--dim', type=int, default=8, help='dimension of user and entity embeddings')
parser.add_argument('-L', '--L', type=int, default=1, help='number of low layers')
parser.add_argument('-H','--H', type=int, default=1, help='number of high layers')
parser.add_argument('-batch_size','--batch_size', type=int, default=4096, help='batch size')
parser.add_argument('-l2_weight','--l2_weight', type=float, default=1e-6, help='weight of l2 regularization')
parser.add_argument('-lr_rs','--lr_rs', type=float, default=0.02, help='learning rate of RS task')
parser.add_argument('-lr_kge','--lr_kge', type=float, default=0.01, help='learning rate of KGE task')
parser.add_argument('-kge_interval','--kge_interval', type=int, default=3, help='training interval of KGE task')
parser.add_argument('-restore','--restore', type=str, default='restore', help='model restore path')
parser.add_argument('-result','--result', type=str, default='result', help='model pd result path')

show_loss = False
show_topk = False

args = parser.parse_args()
data = load_data(args)
train(args, data, show_loss, show_topk)
