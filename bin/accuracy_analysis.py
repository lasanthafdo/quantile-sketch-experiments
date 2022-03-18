import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def produce_bar_plot(mid_q_dict, upper_q_dict, plot_title, x_label, y_label, ylim_top, plot_filename):
    plot_data = [
        ["Mid", mid_q_dict['moments'], mid_q_dict['dds'], mid_q_dict['udds'], mid_q_dict['kll'], mid_q_dict['req']],
        ["Upper", upper_q_dict['moments'], upper_q_dict['dds'], upper_q_dict['udds'], upper_q_dict['kll'],
         upper_q_dict['req']]]
    df = pd.DataFrame(plot_data, columns=["Quantile range", "Moments", "DDS", "UDDS", "KLL", "REQ"])
    ax = df.plot(x="Quantile range", y=["Moments", "DDS", "UDDS", "KLL", "REQ"], kind="bar")
    for p in ax.patches:
        if p.get_height() > ylim_top:
            ax.annotate(str(p.get_height()), (p.get_x() + 0.03, 0.04), rotation=90)

    plt.ylim(0, ylim_top)
    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.legend(loc="lower right")
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots for " + plot_title)


def produce_bar_plot_wt_err_bars(mid_q_dict, upper_q_dict, plot_title, x_label, y_label, ylim_top, plot_filename):
    plot_data = [
        ["Mid", mid_q_dict['moments'], mid_q_dict['dds'], mid_q_dict['udds'], mid_q_dict['kll'], mid_q_dict['req']],
        ["Upper", upper_q_dict['moments'], upper_q_dict['dds'], upper_q_dict['udds'], upper_q_dict['kll'],
         upper_q_dict['req']]]
    data_df = pd.DataFrame(plot_data, columns=["Quantile range", "Moments", "DDS", "UDDS", "KLL", "REQ"])
    data_df = data_df.explode(list(["Moments", "DDS", "UDDS", "KLL", "REQ"]))
    mean_data_df = data_df.groupby('Quantile range', as_index=False).mean().round(4)
    x_ci = data_df.groupby('Quantile range', as_index=False).agg(
        lambda x: np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size))
    print(x_ci)
    ax = mean_data_df.plot(x="Quantile range", y=["Moments", "DDS", "UDDS", "KLL", "REQ"],
                           style=['o-', 'v-', '^-', '|--', 'x--'], kind="bar")
    for i, alg in enumerate(["Moments", "DDS", "UDDS", "KLL", "REQ"]):
        offset = -0.2 + i * 0.1
        ax.errorbar(mean_data_df.index + offset, mean_data_df[alg], label=alg, yerr=x_ci[alg], ecolor='k', capsize=3,
                    linestyle="None")
    # plt.errorbar('Quantile range', ["Moments", "DDS", "UDDS", "KLL", "REQ"], yerr=x_ci, data=mean_data_df, linestyle="None", capsize=3)

    plt.ylim(0, ylim_top)
    plt.xticks(rotation=0)
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
    acc_calc_df = acc_calc_df[
        ['run_id', 'window_id', 'pct_01_acc', 'pct_05_acc', 'pct_25_acc', 'pct_50_acc', 'pct_75_acc',
         'pct_90_acc', 'pct_95_acc', 'pct_98_acc', 'pct_99_acc', 'query_time', 'win_size']]
    acc_calc_df = acc_calc_df.groupby('run_id', as_index=False)[
        ['pct_01_acc', 'pct_05_acc', 'pct_25_acc', 'pct_50_acc', 'pct_75_acc',
         'pct_90_acc', 'pct_95_acc', 'pct_98_acc', 'pct_99_acc', 'query_time', 'win_size']].mean()
    mid_q_all_accs = np.array([acc_calc_df['pct_05_acc'], acc_calc_df['pct_25_acc'],
                               acc_calc_df['pct_50_acc'], acc_calc_df['pct_75_acc'],
                               acc_calc_df['pct_90_acc']]).mean(axis=0)
    upper_q_all_accs = np.array([acc_calc_df['pct_95_acc'], acc_calc_df['pct_98_acc']]).mean(axis=0)
    mid_quantile_accs = [acc_calc_df['pct_05_acc'].mean(), acc_calc_df['pct_25_acc'].mean(),
                         acc_calc_df['pct_50_acc'].mean(), acc_calc_df['pct_75_acc'].mean(),
                         acc_calc_df['pct_90_acc'].mean()]
    upper_quantile_accs = [acc_calc_df['pct_95_acc'].mean(), acc_calc_df['pct_98_acc'].mean()]
    return acc_calc_df, mid_quantile_accs, upper_quantile_accs, mid_q_all_accs, upper_q_all_accs


def plot_hist_dataset(data_df, col_of_interest, bin_width, x_label, y_label, plot_title, plot_file_name):
    data = data_df[col_of_interest]
    # bins=np.arange(min(data), max(data) + bin_width, bin_width)
    # data_df.hist(density=True, column=col_of_interest, bins=50)
    # plt.legend()

    fig, ax = plt.subplots(figsize=(6, 5))
    p = sns.histplot(data=data, stat='probability', ax=ax)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    if plot_title == 'NYT':
        plt.xlim(0, 100)
    plt.savefig(plot_file_name)
    plt.clf()
    print("Finished generating PDF plot for " + plot_title)


if __name__ == '__main__':
    report_folder = sys.argv[1]
    datasets = ['pareto', 'uniform', 'power', 'nyt']
    ds_label_names = {'pareto': 'Pareto', 'uniform': 'Uniform', 'nyt': 'NYT', 'power': 'Power'}
    num_windows = 10
    pd.set_option('display.max_columns', 12)
    data_set_analysis = False
    calc_missing_pct = False
    display_ci = False

    if data_set_analysis:
        nyt_file = '/home/m34ferna/flink-benchmarks/nyt-data.csv'
        nyt_names = ['medallion', 'hack_license', 'vendor_id', 'pickup_datetime', 'payment_type', 'fare_amount',
                     'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount']
        nrows_read = 1000000
        nyt_ds = pd.read_csv(nyt_file, header=None, names=nyt_names, nrows=nrows_read)
        plot_file_name_nyt = report_folder + '/plots/nyt_dist_' + str(nrows_read) + '.png'
        plot_hist_dataset(nyt_ds, 'total_amount', 1, 'Value', 'Probability', 'NYT', plot_file_name_nyt)

        power_file = '/home/m34ferna/flink-benchmarks/household_power_consumption.txt'
        power_names = ["Date", "Time", "Global_active_power", "Global_reactive_power", "Voltage", "Global_intensity",
                       "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"]
        power_ds = pd.read_csv(power_file, header=None, names=power_names, nrows=nrows_read, sep=';')
        plot_file_name_pow = report_folder + '/plots/power_dist_' + str(nrows_read) + '.png'
        plot_hist_dataset(power_ds, 'Global_active_power', 0.01, 'Value', 'Probability', 'Power', plot_file_name_pow)

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
        mid_q_all_dict = {}
        upper_q_all_dict = {}
        algo_ds_acc_dict = {}
        for algo in algos:
            print("Algorithm: " + algo)
            f_algo_ds_real = report_folder + '/' + algo + '-' + dataset + '-real-cleaned.csv'
            f_algo_ds_sketch = report_folder + '/' + algo + '-' + dataset + '-sketch-cleaned.csv'
            algo_ds_real = pd.read_csv(f_algo_ds_real, header=None, names=real_names)
            if calc_missing_pct:
                algo_ds_real['missing_pct'] = algo_ds_real['slack_events'] / algo_ds_real['win_size'] * 100.0
                print(algo_ds_real['missing_pct'])
                missing_pcts.append(np.mean(algo_ds_real[algo_ds_real['missing_pct'] > 0]['missing_pct']))
                print(missing_pcts)

            if algo == 'kll' or algo == 'req':
                algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names_kll_req)
            else:
                algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names)

            algo_ds_sketch['run_id'] = algo_ds_sketch['window_id'].astype(str).str[:4]
            algo_ds_sketch = algo_ds_sketch.groupby('run_id', as_index=False).apply(
                lambda x: x.iloc[:-1] if len(x) > num_windows else x)

            algo_ds_accuracy, mid_q_accuracy, upper_q_accuracy, mid_q_all, upper_q_all = calc_accuracy(algo_ds_sketch,
                                                                                                       algo_ds_real)
            print(algo_ds_accuracy)
            mid_q_dict[algo] = np.round(np.mean(mid_q_accuracy), 4)
            upper_q_dict[algo] = np.round(np.mean(upper_q_accuracy), 4)
            mid_q_all_dict[algo] = mid_q_all
            upper_q_all_dict[algo] = upper_q_all
            algo_ds_acc_dict[algo] = algo_ds_accuracy.drop('run_id', axis=1).mean(axis=1)
            print("==================================================")

        print(">>>>>>>>>>>>> Mid quantile accuracy for " + dataset)
        print(mid_q_all_dict)
        print(">>>>>>>>>>>>> Upper quantile accuracy for " + dataset)
        print(upper_q_all_dict)
        ddsc_mid_diff.append(round(abs(mid_q_dict["dds"] - mid_q_dict["ddsc"]), 4))
        ddsc_upper_diff.append(round(abs(upper_q_dict["dds"] - upper_q_dict["ddsc"]), 4))
        ddsc_upper_diff_pcts.append(
            round(abs(upper_q_dict["dds"] - upper_q_dict["ddsc"]) * 100.0 / upper_q_dict["dds"], 4))
        ddsc_mid_diff_pcts.append(round(abs(mid_q_dict["dds"] - mid_q_dict["ddsc"]) * 100.0 / mid_q_dict["dds"], 4))
        file_ext = '.pdf'
        file_name_suffix = '_avg_errors'
        if display_ci:
            file_name_suffix = file_name_suffix + '_ci'
        plot_file_path = report_folder + '/plots/' + dataset + file_name_suffix + file_ext
        y_axis_top = 0.05
        if display_ci:
            produce_bar_plot_wt_err_bars(mid_q_all_dict, upper_q_all_dict, ds_label_names[dataset], 'Quantiles',
                                         'Avg. Relative Error', y_axis_top, plot_file_path)
        else:
            produce_bar_plot(mid_q_dict, upper_q_dict, ds_label_names[dataset], 'Quantiles',
                             'Avg. Relative Error', y_axis_top, plot_file_path)
        input("Press Enter to continue...")

    if calc_missing_pct:
        print(missing_pcts)
        print(np.mean(missing_pcts))
    print("DDSC - Upper diff pcts:" + str(ddsc_upper_diff_pcts))
    print("DDSC - Mid diff pcts:" + str(ddsc_mid_diff_pcts))
    print("DDSC - Upper diffs:" + str(ddsc_upper_diff))
    print("DDSC - Mid diffs:" + str(ddsc_mid_diff))
