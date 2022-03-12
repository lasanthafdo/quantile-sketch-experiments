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
    print("Finished generating plots for " + plot_title)


def produce_line_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    print(data_df)
    ax = data_df.plot(x=x_axis, y=["Moments", "DDS", "UDDS", "KLL", "REQ"], style=['o-', 'v-', '^-', '|--', 'x--'])
    if x_axis == "Kurtosis":
        ax.set_xscale('symlog')
        ax.set_yscale('log')
        plt.xlim(-2, 1000000)
        # plt.ylim(0, 0.08)
    elif x_axis == "Data Size":
        ax.set_xscale('log')

    # plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.legend(loc="lower right")
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


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
    num_windows = 10
    pd.set_option('display.max_columns', 12)
    nyt_analysis = False

    if nyt_analysis:
        nyt_file = '/home/m34ferna/flink-benchmarks/nyt-data.csv'
        nyt_names = ['medallion', 'hack_license', 'vendor_id', 'pickup_datetime', 'payment_type', 'fare_amount',
                     'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount']
        nyt_ds = pd.read_csv(nyt_file, header=None, names=nyt_names)
        top_k = nyt_ds['total_amount'].value_counts()[:10]
        print(top_k)
        print(top_k.sum())
        # nyt_ds = nyt_ds[(nyt_ds['total_amount'] >= 3.5) & (nyt_ds['total_amount'] <= 12.0)]
        nyt_ds = nyt_ds.groupby('total_amount', as_index=False).count().drop(
            ['medallion', 'hack_license', 'vendor_id', 'pickup_datetime', 'payment_type', 'fare_amount', 'surcharge',
             'mta_tax', 'tip_amount'], axis=1)
        nyt_ds = nyt_ds[nyt_ds['tolls_amount'] >= 2000]
        print(nyt_ds.tail(50))
        # nyt_ds.hist(column='total_amount', bins=100)
        # plt.show()
        # plt.savefig(report_folder + '/plots/nyt_dist.png')
        # plt.clf()
        exit(0)

    missing_pcts = []
    ddsc_upper_diff_pcts = []
    ddsc_mid_diff_pcts = []
    ddsc_upper_diff = []
    ddsc_mid_diff = []
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
        algo_ds_acc_dict = {}
        for algo in algos:
            print("Algorithm: " + algo)
            f_algo_ds_real = report_folder + '/' + algo + '-' + dataset + '-real-cleaned.csv'
            f_algo_ds_sketch = report_folder + '/' + algo + '-' + dataset + '-sketch-cleaned.csv'
            algo_ds_real = pd.read_csv(f_algo_ds_real, header=None, names=real_names)
            algo_ds_real['missing_pct'] = algo_ds_real['slack_events'] / algo_ds_real['win_size'] * 100.0
            print(algo_ds_real['missing_pct'])
            missing_pcts.append(np.mean(algo_ds_real[algo_ds_real['missing_pct'] > 0]['missing_pct']))
            print(missing_pcts)

            if algo == 'kll' or algo == 'req':
                algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names_kll_req)
            else:
                algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names)

            if len(algo_ds_sketch.index) > num_windows:
                algo_ds_sketch.drop(algo_ds_sketch.tail(1).index, inplace=True)

            algo_ds_accuracy, mid_q_accuracy, upper_q_accuracy = calc_accuracy(algo_ds_sketch, algo_ds_real)
            print(algo_ds_accuracy)
            mid_q_dict[algo] = np.round(np.mean(mid_q_accuracy), 4)
            upper_q_dict[algo] = np.round(np.mean(upper_q_accuracy), 4)
            algo_ds_acc_dict[algo] = algo_ds_accuracy.mean(axis=1)
            print("==================================================")

        print(mid_q_dict)
        print(upper_q_dict)
        ddsc_mid_diff.append(round(abs(mid_q_dict["dds"] - mid_q_dict["ddsc"]),4))
        ddsc_upper_diff.append(round(abs(upper_q_dict["dds"] - upper_q_dict["ddsc"]), 4))
        ddsc_upper_diff_pcts.append(round(abs(upper_q_dict["dds"] - upper_q_dict["ddsc"]) * 100.0 / upper_q_dict["dds"], 4))
        ddsc_mid_diff_pcts.append(round(abs(mid_q_dict["dds"] - mid_q_dict["ddsc"]) * 100.0 / mid_q_dict["dds"], 4))
        plot_file_path = report_folder + '/plots/' + dataset + '_avg_errors.png'
        produce_bar_plot(mid_q_dict, upper_q_dict, ds_label_names[dataset], 'Quantile Range', 'Avg. Relative Error',
                         plot_file_path)
        # produce_line_plot(algo_ds_acc_dict,ds_label_names[dataset], "",'Quantile Range', 'Avg. Relative Error', plot_file_path)
        input("Press Enter to continue...")

    print(missing_pcts)
    print(np.mean(missing_pcts))
    print(ddsc_upper_diff_pcts)
    print(ddsc_mid_diff_pcts)
    print(ddsc_upper_diff)
    print(ddsc_mid_diff)
