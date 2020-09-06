import matplotlib.pyplot as plt
import pandas as pd
import pgeocode


class Mining(object):
    def __init__(self, file_path):
        self.df = pd.read_csv(file_path)

    def clean_zipcode(self):
        # split using -
        self.df[["SPRADDR_ZIP_PR", "EXTRA"]] = self.df.SPRADDR_ZIP_PR.str.split(
            expand=True, pat="-"
        )

        # remove all non-numeric rows
        self.df = self.df[self.df.SPRADDR_ZIP_PR.apply(lambda x: str(x).isnumeric())]

        # check length if not 5 then remove entire row
        self.df = self.df[self.df.SPRADDR_ZIP_PR.map(len) == 5]

    def clean_semester(self):
        # split semester column
        self.df[["SEMESTER", "YEAR"]] = self.df.SEMESTER.str.split(expand=True)

    def drop_data(self):
        # remove column and rows as following
        self.df.drop(["HAWAIIAN_LEGACY", "SEMESTER", "EXTRA"], axis=1, inplace=True)
        self.df.dropna(axis=0)

    def clean_df(self):
        self.clean_zipcode()
        self.clean_semester()
        self.drop_data()
        self.df.to_csv("hawaii_enrollments.csv", index=False)

    def get_answers(self):
        df = pd.read_csv("hawaii_enrollments.csv")
        total_students = df["ENROLLMENT"].sum()
        print(
            "Total students were cummulatively enrolled in"
            " total from 2014 to 2019 => `%s` " % total_students
        )
        group_by_campus = (
            df.groupby("IRO_INSTITUTION_DESCL")
            .agg({"ENROLLMENT": sum})
            .sort_values(ascending=False, by="ENROLLMENT")
        )
        highest_students_by_campus = group_by_campus.head(1)
        print(
            "Campus had the most cummulatively enrolled students => `%s` "
            "& Value => `%s` "
            % (
                highest_students_by_campus.index[0],
                highest_students_by_campus["ENROLLMENT"][0],
            )
        )
        least_students_by_campus = group_by_campus.tail(1)
        print(
            "Campus had the least cummulatively enrolled students => `%s` "
            "& Value => `%s` "
            % (
                least_students_by_campus.index[0],
                least_students_by_campus["ENROLLMENT"][0],
            )
        )
        group_by_zip = (
            df.groupby("SPRADDR_ZIP_PR")
            .agg({"ENROLLMENT": sum})
            .sort_values(ascending=False, by="ENROLLMENT")
        )
        highest_students_by_zip = group_by_zip.head(1)
        print(
            "Largest number of students come from => `%s` zipcode"
            % highest_students_by_zip.index[0]
        )
        group_by_manoa_zip = (
            df[df.IRO_INSTITUTION_DESCL == "University of Hawai`i at Manoa"]
            .groupby("SPRADDR_ZIP_PR")
            .agg({"ENROLLMENT": sum})
            .sort_values(ascending=False, by="ENROLLMENT")
        )
        print(
            "Zip code of Manao from where the largest number of students come: => %s"
            % group_by_manoa_zip.index[0]
        )
        unique_zip_manoa = group_by_manoa_zip.index.unique()
        manoa_state_code_df = self.get_state_code_df(unique_zip_manoa)
        state_code_df = group_by_manoa_zip.merge(
            manoa_state_code_df, left_on="SPRADDR_ZIP_PR", right_on="zip"
        )
        state_code_groupby = (
            state_code_df.groupby("state_code").agg({"ENROLLMENT": sum}).reset_index()
        )
        state_code_non_hi = state_code_groupby[state_code_groupby.state_code != "HI"]
        state_code_non_hi.to_csv("zip_code_summary.csv", index=False)
        state_code_non_hi_sorted = state_code_non_hi.sort_values(
            ascending=False, by="ENROLLMENT"
        )
        ax = state_code_non_hi_sorted.head(15).plot.bar(
            x="state_code", y="ENROLLMENT", rot=0
        )
        ax.figure.savefig("state_code_enrollment.png")

    @staticmethod
    def get_state_code_df(uniq_zip):
        nomi = pgeocode.Nominatim("us")
        mapping = []
        for zip in uniq_zip:
            mapping.append(
                {"zip": zip, "state_code": nomi.query_postal_code(zip)["state_code"]}
            )
        return pd.DataFrame(mapping)

    def run(self):
        self.clean_df()
        self.get_answers()


obj = Mining("./enrollment-by-zipcode.csv")
obj.run()
