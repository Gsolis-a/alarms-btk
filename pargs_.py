import argparse




parser = argparse.ArgumentParser(description='A script to check if the alarms are alarmables on the corresponding threshold')
parser.add_argument('-sl', nargs= 1, type=int, help='To slack notification')
# parser.add_argument('-sm', nargs= 1, type=int, help='Alarm scan minutes')
# parser.add_argument('-mt', nargs= 1, type=int, help=' Alarm minimun total')
# parser.add_argument('-at', nargs= 1, type=int, help='Alarm threshold')
# parser.add_argument('-tt', nargs='?', type=int, help='Trigger list timer')
args = parser.parse_args()



print(type(args.sl[0]))