import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import sys


def produce_cdf_plot(data, x_label, y_label, plot_title, filename):
    x = np.sort(data)
    x_range = np.amax(x) - np.amin(x)
    y = np.arange(1, len(x) + 1) / float(len(x))
    percentile50 = np.percentile(x, 50)
    percentile95 = np.percentile(x, 95)
    percentile99 = np.percentile(x, 99)
    plt.plot(x, y, color='red', ls="--", label="CDF")
    plt.axvline(x=percentile50, color='k', linestyle='--')
    plt.text(percentile50 + x_range / 100, 0.6, '50th percentile', rotation=90)
    plt.axvline(x=percentile95, color='b', linestyle='--')
    plt.text(percentile95 + x_range / 100, 0.6, '95th percentile', rotation=90)
    plt.axvline(x=percentile99, color='g', linestyle='--')
    plt.text(percentile99 + x_range / 100, 0.6, '99th percentile', rotation=90)
    # weights = np.ones_like(data) / len(data)
    # plt.hist(data, bins=100, weights=weights, label='Probability density function', alpha=0.8)
    sns.histplot(data, stat='probability', binwidth=100, label="PMF")
    if np.amax(x) <= 1500:
        plt.xlim(0, 1500)
    else:
        plt.xlim(0, 2500)
    plt.title(plot_title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig(filename)
    # plt.show()
    plt.clf()


def produce_bar_plot(mid_q_dict, upper_q_dict, plot_title, x_label, y_label, plot_filename):
    plot_data = [
        ["Mid", mid_q_dict['moments'], mid_q_dict['dds'], mid_q_dict['kll'], mid_q_dict['req'], mid_q_dict['udds']],
        ["Upper", upper_q_dict['moments'], upper_q_dict['dds'], upper_q_dict['kll'], upper_q_dict['req'],
         upper_q_dict['udds']]]
    df = pd.DataFrame(plot_data, columns=["Quantile range", "Moments", "DDS", "KLL", "REQ", "UDDS"])
    print(df)
    ax = df.plot(x="Quantile range", y=["Moments", "DDS", "KLL", "REQ", "UDDS"], kind="bar")
    for p in ax.patches:
        ax.annotate(str(p.get_height()), (p.get_x() + 0.05, 0.01), rotation=90)
    # y = pd.concat([pd.concat([df.iloc[:, 1], df.iloc[:, 2]]), df.iloc[:, 3]])
    # print(y)
    # for i, v in enumerate(y):
    #     plt.text(v + 3, i + .25, str(v), color='blue', fontweight='bold')

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


def produce_imq_bar_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename, y_limit=0.0):
    ax = data_df.plot(x=x_axis, y=["Moments", "DDS", "KLL", "REQ", "UDDS"], kind="bar")
    if y_limit > 0:
        for p in ax.patches:
            ax.annotate(str(p.get_height()), (p.get_x() + 0.03, y_limit / 5), rotation=90)
        plt.ylim(0, y_limit)

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


def produce_imq_line_plot(data_df, plot_title, x_axis, x_label, y_label, plot_filename):
    ax = data_df.plot(x=x_axis, y=["Moments", "DDS", "KLL", "REQ", "UDDS"], style=['o-', 'v-', '|--', 'x--', '^-'])
    if x_axis == "Kurtosis":
        ax.set_xscale('symlog')
        plt.xlim(-2, 1000000)
        plt.ylim(0, 0.08)
    # plt.xticks(rotation=0)
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

    f_query_times = report_folder + '/query_times.csv'
    query_times_df = pd.read_csv(f_query_times)
    print(query_times_df)
    q_plot_file_name = report_folder + '/plots/query_times.png'
    produce_imq_line_plot(query_times_df, 'Query time', "Data Size", 'Data size', 'Time (microseconds)',
                          q_plot_file_name)

    f_insert_times = report_folder + '/insert_times.csv'
    insert_times_df = pd.read_csv(f_insert_times)
    insert_times_df.iloc[:, 1:] = insert_times_df.iloc[:, 1:].div(insert_times_df['Data Size'], axis=0)
    insert_times_df['Data Size'] = insert_times_df['Data Size'] // 1000000
    print(insert_times_df)
    i_plot_file_name = report_folder + '/plots/insert_times.png'
    produce_imq_bar_plot(insert_times_df, 'Insertion time for each data point', "Data Size", 'Data size (millions)',
                         'Time (microseconds)', i_plot_file_name)

    f_merge_times = report_folder + '/merge_times.csv'
    merge_times_df = pd.read_csv(f_merge_times)
    print(merge_times_df)
    i_plot_file_name = report_folder + '/plots/merge_times.png'
    produce_imq_bar_plot(merge_times_df, 'Merge time', "Num Sketches", 'Number of sketches', 'Time (microseconds)',
                         i_plot_file_name, 25000)

    f_kurtosis = report_folder + '/kurtosis.csv'
    kurtosis_df = pd.read_csv(f_kurtosis)
    print(kurtosis_df)
    kurtosis_df.iloc[:, 3:] = kurtosis_df.iloc[:, 3:].sub(kurtosis_df['Actual'], axis=0).div(
        kurtosis_df['Actual'], axis=0).abs().round(4)
    kurtosis_df = kurtosis_df.sort_values(by=['Kurtosis'])
    print(kurtosis_df)
    k_plot_file_name = report_folder + '/plots/kurtosis.png'
    produce_imq_line_plot(kurtosis_df, 'Kurtosis vs Accuracy', "Kurtosis", 'Kurtosis',
                          'Avg. Relative Error (98th percentile)', k_plot_file_name)

    f_adaptability = report_folder + '/adaptability.csv'
    adaptability_df = pd.read_csv(f_adaptability)
    adaptability_df.iloc[:, 2:] = adaptability_df.iloc[:, 2:].sub(adaptability_df['Actual'], axis=0).div(
        adaptability_df['Actual'], axis=0).abs().round(4)
    print(adaptability_df)
    adaptability_df.loc[adaptability_df['Quantile'] != 0.5, 'Quantile'] = 0
    adaptability_df.loc[adaptability_df['Quantile'] == 0.5, 'Quantile'] = 1
    adaptability_df = adaptability_df.groupby('Quantile', as_index=False).mean().round(4)
    print(adaptability_df)
    a_plot_file_name = report_folder + '/plots/adaptability.png'
    produce_imq_bar_plot(adaptability_df, 'Adaptability', "Quantile", 'Quantiles',
                         'Avg. Relative Error', a_plot_file_name, 0.05)
    # for dataset in datasets:
    #     algos = ['moments', 'dds', 'kll', 'req', 'udds']
    #     real_names = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95', 'pct_98',
    #                   'pct_99',
    #                   'win_size', 'slack_events']
    #     sketch_names = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95', 'pct_98',
    #                     'pct_99', 'query_time']
    #     sketch_names_kll_req = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95', 'pct_98',
    #                         'pct_99', 'query_time', 'num_retained']
    #     mid_q_dict = {}
    #     upper_q_dict = {}
    #     for algo in algos:
    #         f_algo_ds_real = report_folder + '/' + algo + '-' + dataset + '-real-cleaned.csv'
    #         f_algo_ds_sketch = report_folder + '/' + algo + '-' + dataset + '-sketch-cleaned.csv'
    #         algo_pareto_real = pd.read_csv(f_algo_ds_real, header=None, names=real_names)
    #
    #         if algo == 'kll' or algo == 'req':
    #             algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names_kll_req)
    #         else:
    #             algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names)
    #
    #         if len(algo_ds_sketch.index) > num_windows:
    #             algo_ds_sketch.drop(algo_ds_sketch.tail(1).index, inplace=True)
    #
    #         algo_ds_accuracy, mid_q_accuracy, upper_q_accuracy = calc_accuracy(algo_ds_sketch, algo_pareto_real)
    #         print(algo_ds_accuracy)
    #         mid_q_dict[algo] = np.round(np.mean(mid_q_accuracy), 4)
    #         upper_q_dict[algo] = np.round(np.mean(upper_q_accuracy), 4)
    #
    #     print(mid_q_dict)
    #     print(upper_q_dict)
    #     plot_file_path = report_folder + '/plots/' + dataset + '.png'
    #     produce_bar_plot(mid_q_dict, upper_q_dict, ds_label_names[dataset], 'Quantile Range', 'Avg. Relative Error',
    #                      plot_file_path)
    #     input("Press Enter to continue...")
