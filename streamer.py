from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
from google.cloud import bigquery
import apache_beam as beam
import logging
import argparse
import sys
import re

PROJECT = "stream-267306"
# schema = 'date_of_transaction:STRING,store_number:STRING,category_number:STRING,category_main_group_number:STRING,date_year:STRING,date_month:STRING,seasonal_flag:STRING,weekdaynumber:STRING,lookupmonthid:STRING,date_of_opening:STRING,date_of_closing:STRING,location_type:STRING,sum_quantity:STRING,fis_sayisi:STRING,isweekend:STRING,discount_net:STRING,monthly_category_total_sell:STRING,monthly_count_day_of_sell:STRING,monthly_avg_store_category_sell:STRING,sum_store_amount:STRING,satis_gun_sayisi:STRING,ort_store_satis_monthday:STRING,gorinan_flag:STRING,gorinan_flag_zeytinyagi:STRING,gorinan_flag_tuvalet_kagidi:STRING,gorinan_flag_havlu_kagidi:STRING,gorinan_flag_deterjan:STRING,gorinan_flag_aycicek:STRING,perc_90:STRING,perc_80:STRING,perc_70:STRING,perc_60:STRING,perc_50:STRING,perc_40:STRING,perc_30:STRING,perc_20:STRING,ntanealana_percx_indirim:STRING,ntanealana_mtane_bedava:STRING,asure_ayi:STRING,kurban_arife:STRING,kurban_1:STRING,kurban_2:STRING,kurban_3:STRING,kurban_4:STRING,oruc_arifesi:STRING,oruc_gunu:STRING,ramazan_arifesi:STRING,ramazan_1:STRING,ramazan_2:STRING,ramazan_3:STRING,anneler_gunu:STRING,babalar_gunu:STRING,yilbasi_arife:STRING,yilbasi_oncesi:STRING,yilbasi:STRING,yilin_ilk_gunu:STRING,subat_14:STRING,kadinlar_gunu:STRING,mart_8:STRING,nisan_23:STRING,mayis_1:STRING,mayis_19:STRING,temmuz_15:STRING,agustos_30:STRING,ekim_29:STRING,kasim_10:STRING,karne_gunu:STRING,karne_haftasi:STRING,okul_acilisi:STRING,okul_acilis_oncesi:STRING,okul_acilis_sonrasi:STRING,label:STRING'
schema = 'date_of_transaction:DATETIME,store_number:INTEGER,category_number:INTEGER,category_main_group_number:INTEGER,date_year:INTEGER,date_month:INTEGER,seasonal_flag:INTEGER,weekdaynumber:INTEGER,lookupmonthid:INTEGER,date_of_opening:DATETIME,date_of_closing:DATETIME,location_type:INTEGER,sum_quantity:FLOAT,fis_sayisi:INTEGER,isweekend:INTEGER,discount_net:FLOAT,monthly_category_total_sell:FLOAT,monthly_count_day_of_sell:INTEGER,monthly_avg_store_category_sell:FLOAT,sum_store_amount:FLOAT,satis_gun_sayisi:INTEGER,ort_store_satis_monthday:FLOAT,gorinan_flag:INTEGER,gorinan_flag_zeytinyagi:INTEGER,gorinan_flag_tuvalet_kagidi:INTEGER,gorinan_flag_havlu_kagidi:INTEGER,gorinan_flag_deterjan:INTEGER,gorinan_flag_aycicek:INTEGER,perc_90:INTEGER,perc_80:INTEGER,perc_70:INTEGER,perc_60:INTEGER,perc_50:INTEGER,perc_40:INTEGER,perc_30:INTEGER,perc_20:INTEGER,ntanealana_percx_indirim:INTEGER,ntanealana_mtane_bedava:INTEGER,asure_ayi:INTEGER,kurban_arife:INTEGER,kurban_1:INTEGER,kurban_2:INTEGER,kurban_3:INTEGER,kurban_4:INTEGER,oruc_arifesi:INTEGER,oruc_gunu:INTEGER,ramazan_arifesi:INTEGER,ramazan_1:INTEGER,ramazan_2:INTEGER,ramazan_3:INTEGER,anneler_gunu:INTEGER,babalar_gunu:INTEGER,yilbasi_arife:INTEGER,yilbasi_oncesi:INTEGER,yilbasi:INTEGER,yilin_ilk_gunu:INTEGER,subat_14:INTEGER,kadinlar_gunu:INTEGER,mart_8:INTEGER,nisan_23:INTEGER,mayis_1:INTEGER,mayis_19:INTEGER,temmuz_15:INTEGER,agustos_30:INTEGER,ekim_29:INTEGER,kasim_10:INTEGER,karne_gunu:INTEGER,karne_haftasi:INTEGER,okul_acilisi:INTEGER,okul_acilis_oncesi:INTEGER,okul_acilis_sonrasi:INTEGER,label:FLOAT'
TOPIC = "projects/stream-267306/topics/stream"


class Split(beam.DoFn):

    def process(self, element):
        from datetime import datetime
        element = element.split(",")

        return [{
            'date_of_transaction': element[0],
            'store_number': element[1],
            'category_number': element[2],
            'category_main_group_number': element[3],
            'date_year': element[4],
            'date_month': element[5],
            'seasonal_flag': element[6],
            'weekdaynumber': element[7],
            'lookupmonthid': element[8],
            'date_of_opening': element[9],
            'date_of_closing': element[10],
            'location_type': element[11],
            'sum_quantity': element[12],
            'fis_sayisi': element[13],
            'isweekend': element[14],
            'discount_net': element[15],
            'monthly_category_total_sell': element[16],
            'monthly_count_day_of_sell': element[17],
            'monthly_avg_store_category_sell': element[18],
            'sum_store_amount': element[19],
            'satis_gun_sayisi': element[20],
            'ort_store_satis_monthday': element[21],
            'gorinan_flag': element[22],
            'gorinan_flag_zeytinyagi': element[23],
            'gorinan_flag_tuvalet_kagidi': element[24],
            'gorinan_flag_havlu_kagidi': element[25],
            'gorinan_flag_deterjan': element[26],
            'gorinan_flag_aycicek': element[27],
            'perc_90': element[28],
            'perc_80': element[29],
            'perc_70': element[30],
            'perc_60': element[31],
            'perc_50': element[32],
            'perc_40': element[33],
            'perc_30': element[34],
            'perc_20': element[35],
            'ntanealana_percx_indirim': element[36],
            'ntanealana_mtane_bedava': element[37],
            'asure_ayi': element[38],
            'kurban_arife': element[39],
            'kurban_1': element[40],
            'kurban_2': element[41],
            'kurban_3': element[42],
            'kurban_4': element[43],
            'oruc_arifesi': element[44],
            'oruc_gunu': element[45],
            'ramazan_arifesi': element[46],
            'ramazan_1': element[47],
            'ramazan_2': element[48],
            'ramazan_3': element[49],
            'anneler_gunu': element[50],
            'babalar_gunu': element[51],
            'yilbasi_arife': element[52],
            'yilbasi_oncesi': element[53],
            'yilbasi': element[54],
            'yilin_ilk_gunu': element[55],
            'subat_14': element[56],
            'kadinlar_gunu': element[57],
            'mart_8': element[58],
            'nisan_23': element[59],
            'mayis_1': element[60],
            'mayis_19': element[61],
            'temmuz_15': element[62],
            'agustos_30': element[63],
            'ekim_29': element[64],
            'kasim_10': element[65],
            'karne_gunu': element[66],
            'karne_haftasi': element[67],
            'okul_acilisi': element[68],
            'okul_acilis_oncesi': element[69],
            'okul_acilis_sonrasi': element[70],
            'label': element[71]

        }]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_topic")
    parser.add_argument("--output")
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p
     | 'ReadData' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
     | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
     # | "Clean Data" >> beam.Map(regex_clean)
     | 'ParseCSV' >> beam.ParDo(Split())
     | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:stream.stream1b'.format(PROJECT), schema=schema,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
     )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    main()