import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def produce_bar_plot(mid_q_dict, upper_q_dict, plot_title, x_label, y_label, plot_filename):
    plot_data = [
        ["Mid", mid_q_dict['moments'], mid_q_dict['dds'], mid_q_dict['udds'], mid_q_dict['kll'], mid_q_dict['req']],
        ["Upper", upper_q_dict['moments'], upper_q_dict['dds'], upper_q_dict['udds'], upper_q_dict['kll'],
         upper_q_dict['req']]]
    df = pd.DataFrame(plot_data, columns=["Quantile range", "Moments", "DDS", "UDDS", "KLL", "REQ"])
    print(df)
    ax = df.plot(x="Quantile range", y=["Moments", "DDS", "UDDS", "KLL", "REQ"], kind="bar")
    for p in ax.patches:
        if p.get_height() > 0.06:
            ax.annotate(str(p.get_height()), (p.get_x() + 0.03, 0.04), rotation=90)

    plt.ylim(0, 0.06)
    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.legend(loc="lower right")
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")
    # plt.show()
    plt.clf()


def calc_accuracy(approx_df, real_df):
    acc_calc_df = pd.merge(approx_df, real_df, on='window_id', suffixes=("_approx", "_real"))
    acc_calc_df['pct_01_acc'] = abs(
        acc_calc_df.pct_01_approx - acc_calc_df.pct_01_real) / acc_calc_df.pct_01_real
    acc_calc_df['pct_05_acc'] = abs(
        acc_calc_df.pct_05_approx - acc_calc_df.pct_05_real) / acc_calc_df.pct_05_real
    acc_calc_df['pct_25_acc'] = abs(
        acc_calc_df.pct_25_approx - acc_calc_df.pct_25_real) / acc_calc_df.pct_25_real
    acc_calc_df['pct_50_acc'] = abs(
        acc_calc_df.pct_50_approx - acc_calc_df.pct_50_real) / acc_calc_df.pct_50_real
    acc_calc_df['pct_75_acc'] = abs(
        acc_calc_df.pct_75_approx - acc_calc_df.pct_75_real) / acc_calc_df.pct_75_real
    acc_calc_df['pct_90_acc'] = abs(
        acc_calc_df.pct_90_approx - acc_calc_df.pct_90_real) / acc_calc_df.pct_90_real
    acc_calc_df['pct_95_acc'] = abs(
        acc_calc_df.pct_95_approx - acc_calc_df.pct_95_real) / acc_calc_df.pct_95_real
    acc_calc_df['pct_98_acc'] = abs(
        acc_calc_df.pct_98_approx - acc_calc_df.pct_98_real) / acc_calc_df.pct_98_real
    acc_calc_df['pct_99_acc'] = abs(
        acc_calc_df.pct_99_approx - acc_calc_df.pct_99_real) / acc_calc_df.pct_99_real
    acc_calc_df = acc_calc_df[['window_id', 'pct_01_acc', 'pct_05_acc', 'pct_25_acc', 'pct_50_acc', 'pct_75_acc',
                               'pct_90_acc', 'pct_95_acc', 'pct_98_acc', 'pct_99_acc', 'query_time', 'win_size']]
    mid_quantile_accs = [acc_calc_df['pct_01_acc'].mean(), acc_calc_df['pct_05_acc'].mean(),
                         acc_calc_df['pct_25_acc'].mean(), acc_calc_df['pct_50_acc'].mean(),
                         acc_calc_df['pct_75_acc'].mean(), acc_calc_df['pct_90_acc'].mean()]
    upper_quantile_accs = [acc_calc_df['pct_95_acc'].mean(), acc_calc_df['pct_98_acc'].mean()]
    return acc_calc_df, mid_quantile_accs, upper_quantile_accs


if __name__ == '__main__':
    report_folder = sys.argv[1]
    datasets = ['pareto', 'uniform', 'power', 'nyt']
    ds_label_names = {'pareto': 'Pareto', 'uniform': 'Uniform', 'nyt': 'NYT', 'power': 'Power'}
    num_windows = 5

    for dataset in datasets:
        algos = ['moments', 'dds', 'ddsc', 'kll', 'req', 'udds']
        real_names = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95', 'pct_98',
                      'pct_99',
                      'win_size', 'slack_events']
        sketch_names = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95', 'pct_98',
                        'pct_99', 'query_time']
        sketch_names_kll_req = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95',
                                'pct_98',
                                'pct_99', 'query_time', 'num_retained']
        mid_q_dict = {}
        upper_q_dict = {}
        for algo in algos:
            f_algo_ds_real = report_folder + '/' + algo + '-' + dataset + '-real-cleaned.csv'
            f_algo_ds_sketch = report_folder + '/' + algo + '-' + dataset + '-sketch-cleaned.csv'
            algo_pareto_real = pd.read_csv(f_algo_ds_real, header=None, names=real_names)

            if algo == 'kll' or algo == 'req':
                algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names_kll_req)
            else:
                algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names)

            if len(algo_ds_sketch.index) > num_windows:
                algo_ds_sketch.drop(algo_ds_sketch.tail(1).index, inplace=True)

            algo_ds_accuracy, mid_q_accuracy, upper_q_accuracy = calc_accuracy(algo_ds_sketch, algo_pareto_real)
            print(algo_ds_accuracy)
            mid_q_dict[algo] = np.round(np.mean(mid_q_accuracy), 4)
            upper_q_dict[algo] = np.round(np.mean(upper_q_accuracy), 4)

        print(mid_q_dict)
        print(upper_q_dict)
        plot_file_path = report_folder + '/plots/' + dataset + '.pdf'
        produce_bar_plot(mid_q_dict, upper_q_dict, ds_label_names[dataset], 'Quantile Range', 'Avg. Relative Error',
                         plot_file_path)
        input("Press Enter to continue...")
