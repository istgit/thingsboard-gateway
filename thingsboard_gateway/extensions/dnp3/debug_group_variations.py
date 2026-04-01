#!/usr/bin/env python3
"""
DNP3 Group Variation Debugger
This script connects to a DNP3 outstation and logs what GroupVariations it receives.
Use this to figure out what to put in your profile CSV.
"""

import time
from pydnp3 import opendnp3, openpal, asiopal, asiodnp3
from thingsboard_gateway.extensions.dnp3.visitorindexedbinary import *


class DebugSOEHandler(opendnp3.ISOEHandler):
    """SOE Handler that logs all received GroupVariations"""

    def __init__(self):
        super().__init__()
        self.received_gvs = set()

    def Process(self, info, values):
        gv_str = str(info.gv).split('.')[-1]

        if info.gv not in self.received_gvs:
            self.received_gvs.add(info.gv)
            print(f"\n🔍 NEW GroupVariation detected: {gv_str}")
            print(f"   Qualifier: {info.qualifier}")
            print(f"   Is Event: {info.isEventVariation}")

        # Try to get indices
        try:
            # Create appropriate visitor based on type
            if hasattr(values, 'Count'):
                count = values.Count()
                print(f"   📊 {gv_str}: {count} values received")

                # Try to iterate if possible
                

                visitor_map = {
                    opendnp3.ICollectionIndexedBinary: VisitorIndexedBinary,
                    opendnp3.ICollectionIndexedDoubleBitBinary: VisitorIndexedDoubleBitBinary,
                    opendnp3.ICollectionIndexedCounter: VisitorIndexedCounter,
                    opendnp3.ICollectionIndexedAnalog: VisitorIndexedAnalogTime,
                    opendnp3.ICollectionIndexedBinaryOutputStatus: VisitorIndexedBinaryOutputStatus,
                    opendnp3.ICollectionIndexedAnalogOutputStatus: VisitorIndexedAnalogOutputStatus,
                }

                visitor_class = visitor_map.get(type(values))
                if visitor_class:
                    visitor = visitor_class()
                    values.Foreach(visitor)

                    indices = [item[0] for item in visitor.index_and_value[:5]]  # Show first 5
                    values_sample = [item[1] for item in visitor.index_and_value[:5]]

                    print(f"   📝 Sample indices: {indices}")
                    print(f"   💾 Sample values: {values_sample}")
                    print(f"   ✅ Add these to your profile CSV:")
                    for idx in indices:
                        print(f"      {gv_str},{idx},Description here,Your Field Name Here")
        except Exception as e:
            print(f"   ⚠️  Could not read values: {e}")

    def Start(self):
        print("\n▶️  SOE Start")

    def End(self):
        print("⏸️  SOE End\n")


def main():
    print("=" * 60)
    print("DNP3 Group Variation Debugger")
    print("=" * 60)
    print("This tool will connect to your outstation and show you")
    print("what GroupVariations it sends, so you can update your profile CSV.")
    print()

    # Configuration (adjust as needed)
    outstation_ip = input("Enter outstation IP [127.0.0.1]: ").strip() or "127.0.0.1"
    outstation_port = int(input("Enter outstation port [20010]: ").strip() or "20010")
    outstation_id = int(input("Enter outstation ID [10]: ").strip() or "10")
    master_id = int(input("Enter master ID [1000]: ").strip() or "1000")

    print(f"\n🔌 Connecting to {outstation_ip}:{outstation_port} (Outstation ID {outstation_id})")

    # Create DNP3 manager
    manager = asiodnp3.DNP3Manager(1, asiodnp3.ConsoleLogger().Create())

    # Create channel
    channel = manager.AddTCPClient(
        "debugger",
        opendnp3.levels.NORMAL,
        asiopal.ChannelRetry().Default(),
        outstation_ip,
        "0.0.0.0",
        outstation_port,
        asiodnp3.PrintingChannelListener().Create()
    )

    # Create master with debug SOE handler
    stack_config = asiodnp3.MasterStackConfig()
    stack_config.link.LocalAddr = master_id
    stack_config.link.RemoteAddr = outstation_id

    soe_handler = DebugSOEHandler()

    master = channel.AddMaster(
        "debug_master",
        soe_handler,
        asiodnp3.DefaultMasterApplication().Create(),
        stack_config
    )

    # Add integrity scan (Class 0-3)
    master.AddClassScan(
        opendnp3.ClassField().AllClasses(),
        openpal.TimeDuration().Seconds(10),
        opendnp3.TaskConfig().Default()
    )

    master.Enable()

    print("\n✅ Connected! Listening for data...")
    print("   The first poll will show all GroupVariations.")
    print("   Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping...")

    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY: GroupVariations Received")
    print("=" * 60)

    if soe_handler.received_gvs:
        for gv in sorted(soe_handler.received_gvs, key=lambda x: str(x)):
            gv_str = str(gv).split('.')[-1]
            print(f"   • {gv_str}")

        print("\n💡 Next Steps:")
        print("   1. Copy the CSV lines shown above")
        print("   2. Paste them into your DNP3Profile2.csv")
        print("   3. Replace 'Description here' and 'Your Field Name Here' with actual names")
        print("   4. Restart your connector")
    else:
        print("   ⚠️  No data received. Check:")
        print("      - Is the outstation running?")
        print("      - Are master_id and outstation_id correct?")
        print("      - Is the outstation configured to send data?")

    manager.Shutdown()
    print("\n✅ Done!")


if __name__ == "__main__":
    main()