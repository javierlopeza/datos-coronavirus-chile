import os
import pandas as pd
import camelot

from glob import glob

from reporte_parser import reporte_parser

TABLA_IDS = {
    'tabla1_id': 'Casos confirmados de Coronavirus a nivel nacional',
    'tabla2_id': 'PCR \(\+\), sintomáticos o asintomáticos',
    'tabla3_id': 'Pacientes fallecidos por grupos etarios',
    'tabla4_id': 'otal de ventiladores'

}

INPUTH_PATH = '../reporte_parser/input'
OUTPUT_PATH = './ReporteDiario/output/'


class DOReporteParser(reporte_parser.ReporteParser):

    def parse_tables(self, path=INPUTH_PATH):
        tables = camelot.read_pdf(
            INPUTH_PATH + "/tablas_reporte_{}.pdf".format(self.last_reporte_date),
            pages='1-end',
            flavor="stream",
        )
        self.all_tables = tables

    def table_identifier(self):
        '''
        We need to be able to identify each column from reporte diario.
        as of 2020-06-09, there are (in parenthesis their map to MinCiencia's repo, on input/ReporteDiario/):
        1.- Casos confirmados de Coronavirus a nivel nacional (maps to CasosConfirmados.csv )
        2.- Casos confirmados totales, casos recuperados, casos activos y fallecidos (maps to CasosConfirmadosTotales.csv)
        3.- Pacientes fallecidos por grupos etarios (maps to FallecidosEtario.csv)
        4.- Sistema Integrado Covid 19, camas de unidades de cuidados intensivos (maps to NumeroVentiladores.csv)
        5.- Hospitalización en unidades de cuidados intensivos (UCI) de pacientes Covid 19 (tabla regional) (maps to UCI.csv)
        6.- Hospitalización en unidades de cuidados intensivos (UCI) de pacientes Covid 19 (tramos de edad) (maps to HospitalizadosUCIEtario.csv)
        7.- Hospitalización en unidades de cuidados intensivos (UCI) de pacientes Covid 19 (tipo ventilacion) (maps to PacientesVMI .csv)
        8.- Hospitalización de pacientes Covid19 en sistema integrado (maps to CamasHospital_Diario.csv)
        9.- Exámenes PCR (maps to PCREstablecimiento.csv)
        10.- PCR por region deprecated: No longer informed by Minsal (maps to PCR.csv)
        11.- Residencias sanitarias informadas por región(maps to ResidenciasSanitarias.csv)
        12.- NOT A TABLE Pacientes criticos, extracted from a graph (maps to PacientesCriticos.csv)
        :return:
        '''
        print('On ' + self.last_reporte_date + ' got ' + str(len(self.all_tables)) + ' tables')
        # Quik dirty, awful and embarrasing solution: identify the df based on strings :(
        for each_table in self.all_tables:
            my_df = each_table.df
            # df[df['model'].str.match('Mac')]
            # check how to identify each table
            # print(my_df.to_string())

            table_identified = False
            for each_column in my_df.columns:
                if my_df[each_column].str.contains(TABLA_IDS['tabla1_id']).any():
                    print('found table1')
                    table_identified = True
                    self.table_1_composer(my_df)
                elif my_df[each_column].str.contains(TABLA_IDS['tabla2_id']).any():
                    print('found table2')
                    table_identified = True
                    self.table_2_composer(my_df)
                elif my_df[each_column].str.contains(TABLA_IDS['tabla3_id']).any():
                    print('found table3')
                    table_identified = True
                    self.table_3_composer(my_df)
                elif my_df[each_column].str.contains(TABLA_IDS['tabla4_id']).any():
                    print('found table4')
                    table_identified = True
                    self.table_4_composer(my_df)
                    return

            if not table_identified:
                print('Table not identified')
                print(my_df.head(10).to_string())

    def table_1_composer(self, df1):
        '''
        table_identifier encuentra bien la tabla 1, pero el formato es cuestionable
        aca lo ordenamos: La primera columna es fija: regiones, y desde ahi en adelante,
        agregamos todas las columnas.
        La ultima columna ha sido porcentajes. La fila con el 100% la podemos usar para validar,
        y no es necesario guardarla
        :return:
        '''
        output_file = OUTPUT_PATH + self.last_reporte_date + '_table1.csv'
        if os.path.isfile(output_file):
            print(output_file + ' was processed, won\'t do anything. If you want to reprocess, rename/remove the csv')
            return

        # 1.- drop row con 'Casos confirmados de Coronavirus a nivel nacional'
        for each_column in df1.columns:
            df1 = df1[~df1[each_column].str.contains(TABLA_IDS['tabla1_id'])]
            # also, drop row with explanation (footer)
            df1 = df1[~df1[each_column].str.contains('Epivigila')]
            # in the column titles, there's a \n which sometimes spans empty cells, and always splits some titles
            #

        # we can identify data based on Region name and 100% (both corners)
        # everything before that is a header
        # BOTH these lists must have a single element
        data_start_index = df1.index[df1[0] == 'Arica - Parinacota'].tolist()[0]
        data_end_index = df1.index[df1[6] == '100%'].tolist()[0]
        print('Table 1 data starts at index ' + str(data_start_index) + ' and ends at ' + str(data_end_index))
        # print(df1.to_string())

        header = df1.iloc[0:data_start_index - 1, ]
        header = header.replace(r'\\n', '', regex=True)

        proper_data = df1.iloc[data_start_index - 1:, ]
        proper_data.reset_index(drop=True, inplace=True)
        proper_data.iat[len(proper_data.index) - 1, 0] = 'Total'

        # print(proper_data)

        # concat per column, ignore empty. The goal is to have a proper title in the third row
        for i in range(0, len(header.index) - 1):
            for j in range(0, len(header.columns)):
                if header.iloc[i + 1, j] != '':
                    header.iloc[i + 1, j] = str(header.iloc[i, j]) + ' ' + str(header.iloc[i + 1, j])
                else:
                    header.iloc[i + 1, j] = header.iloc[i, j]

        header = header.replace(r'  ', ' ', regex=True)
        # header.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        # header.iloc[len(header.index), :] = header.iloc[len(header.index), :].str.strip()

        proper_header = header.iloc[len(header.index) - 1:len(header.index), :]
        proper_header[0] = 'Region'

        df_table = pd.concat([proper_header, proper_data])
        df_table.to_csv(output_file, index=False, header=False)

    def table_2_composer(self, df2):
        '''
        table_identifier encuentra bien la tabla 2, pero el formato es cuestionable
        aca lo ordenamos: La primera columna tiene fechas
        La ultima columna ha sido porcentajes. La fila con el 100% la podemos usar para validar,
        y no es necesario guardarla
        :return:
        '''
        output_file = OUTPUT_PATH + self.last_reporte_date + '_table2.csv'
        if os.path.isfile(output_file):
            print(output_file + ' was processed, won\'t do anything. If you want to reprocess, rename/remove the csv')
            return

        # 1.- drop row con 'Casos confirmados totales'
        for each_column in df2.columns:
            df2 = df2[~df2[each_column].str.contains(TABLA_IDS['tabla2_id'])]
            # also, drop row with explanation (footer)
            df2 = df2[~df2[each_column].str.contains('MINSAL')]
            df2 = df2[~df2[each_column].str.contains('Epivigila')]
            # also drop those containing 'sintomáticos o asintomáticos'
            df2 = df2[~df2[each_column].str.contains('Casos confirmados totales, casos recuperados,')]
            # up to here, all garbage rows were dropped

        # to identify data:
        # left-up corner: first date
        # right-down corner: last number or percentage
        dates_idx = df2.apply(lambda x: x.str.match(r'\d{2}-\d{2}-\d{4}')).values.nonzero()
        dates_idx = dates_idx[0]
        # dates are the indexes of those files with dates
        data_start_index = dates_idx[0]
        data_end_index = dates_idx[len(dates_idx) - 1]
        print('Table 2 data starts at index ' + str(data_start_index) + ' and ends at ' + str(data_end_index))
        # print(df2.to_string())

        header = df2.iloc[0:data_start_index, ]
        header = header.replace(r'\\n', '', regex=True)
        header = header.replace(r'\*', '', regex=True)
        # print(header.to_string())

        proper_data = df2.iloc[data_start_index:, ]
        proper_data.reset_index(drop=True, inplace=True)

        # print(proper_data)

        # concat per column, ignore empty. The goal is to have a proper title in the third row
        for i in range(0, len(header.index) - 1):
            for j in range(0, len(header.columns)):
                header.iloc[i + 1, j] = str(header.iloc[i, j]) + ' ' + str(header.iloc[i + 1, j])

        header = header.replace(r'  ', ' ', regex=True)
        # header.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        header.iloc[len(header.index) - 1, :] = header.iloc[len(header.index) - 1, :].str.strip()

        # headers are messi: there are too many spaces, but it looks deterministic
        header = header.replace({'Ca s os total es a cumul a dos': 'Casos totales acumulados',
                                 'Ca s os tota l es a cumul a dos': 'Casos totales acumulados',
                                 'Ca s os a ctivos': 'Casos activos',
                                 'Ca s os a cti vos': 'Casos activos',
                                 'Ca s os recupera dos': 'Casos recuperados',
                                 'Fa l l eci dos': 'Fallecidos',
                                 'Ca s os nuevos total es': 'Casos nuevos totales',
                                 'Ca s os nuevos tota l es': 'Casos nuevos totales',
                                 'Ca s os nuevos con s íntoma s': 'Casos nuevos con síntomas',
                                 'Ca s os nuevos s i n s íntoma s *': 'Casos nuevos sin síntomas',
                                 '% Aumento di a ri o': '% Aumento diario',
                                 'Nuevos recupera dos': 'Nuevos recuperados'
                                 }, regex=True)


        # print(header.to_string())
        proper_header = header.iloc[len(header.index) - 1:len(header.index), :]

        #print(proper_header.to_string())

        df_table = pd.concat([proper_header, proper_data])
        df_table.to_csv(output_file, index=False, header=False)

    def table_3_composer(self, df3):
        '''
        table_identifier encuentra bien la tabla 3, el formato es aceptable
        Limites de datos:
        'Tramos de edad': esquina superior izquierda
        Ultimo 100%: Esquina inferior derecha

        :return:
        '''
        output_file = OUTPUT_PATH + self.last_reporte_date + '_table3.csv'
        if os.path.isfile(output_file):
            print(output_file + ' was processed, won\'t do anything. If you want to reprocess, rename/remove the csv')
            return

        # 1.- drop row con 'Casos confirmados de Coronavirus a nivel nacional'
        for each_column in df3.columns:
            df3 = df3[~df3[each_column].str.contains(TABLA_IDS['tabla3_id'])]

        df3.to_csv(output_file, index=False, header=False)

    def table_4_composer(self, df4):
        '''
        table_identifier encuentra la tabla 4, pero es un desastre: tabla 4 a 7 estan mezcladas
        Limites de datos:
        Tabla 4 es una fila, que comienza con 'Total de ventiladores'

        :return:
        '''
        output_file = OUTPUT_PATH + self.last_reporte_date + '_table4.csv'
        if os.path.isfile(output_file):
            print(output_file + ' was processed, won\'t do anything. If you want to reprocess, rename/remove the csv')
            return
        print(df4.to_string())
        # 1.-Get only row con 'Total de ventiladores
        for each_column in df4.columns:
            df4 = df4[df4[each_column].str.contains(TABLA_IDS['tabla4_id'])]

        #print(df4.to_string())


    def parse_all_reportes(self):
        # parser.load_input()
        # parser.download_reporte()
        for each_pdf in glob(INPUTH_PATH + '/*.pdf'):
            # files are identified by date
            date = each_pdf.replace(INPUTH_PATH + '/tablas_reporte_', '').replace('.pdf', '')
            print('parsing ' + each_pdf)
            self.last_reporte_date = date
            self.parse_tables()
            self.table_identifier()

if __name__ == '__main__':
    DOparser = DOReporteParser()
    DOparser.parse_all_reportes()


