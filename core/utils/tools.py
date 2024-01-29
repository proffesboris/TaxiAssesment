import sys


def handle_args():

    st_dt = "2023-01"
    end_dt = None

    if len(sys.argv) > 2:

        check1 = int(sys.argv[1].split("-")[0]) > int(sys.argv[2].split("-")[0])
        check2 = int(sys.argv[1].split("-")[1]) > int(sys.argv[2].split("-")[1])

        if (check1 and check2) or (check1):
            raise Exception("\n Start date is bigger then end date, pls fix! \n"
                            "First date should be lower then second \n")

        else:
            print(sys.argv)
            print(f"start date for handling: {sys.argv[1]} \n"
                      f"end date for handling: {sys.argv[2]}")

            st_dt = str(sys.argv[1])
            end_dt = str(sys.argv[2])

    elif len(sys.argv) > 1:

        st_dt = sys.argv[1]

    print("take default_date: '2023-01' ")

    return st_dt, end_dt


def build_list_of_dates(st, en):

    list_of_dates = []

    st_y, st_m = int(st.split('-')[0]), int(st.split('-')[1])

    if en is not None:
        en_y, en_m = int(en.split('-')[0]), int(en.split('-')[1])

        check = st_y < en_y

        if check:

            for y in range(st_y, en_y):

                for d in range(10, 13):
                    list_of_dates.append(str(y) + "-" + str(d).zfill(2))

        for m in range(st_m, en_m + 1):

            if st_y == en_y:

                list_of_dates.append(str(st_y) + "-" + str(m).zfill(2))

            elif check:

                for y in range(st_y, en_y + 1):
                    list_of_dates.append(str(y) + "-" + str(m).zfill(2))

    else:
        list_of_dates.append(st)

    print(list_of_dates)

    return list_of_dates
