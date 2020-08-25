#!/usr/bin/env python3

from aws_cdk import core

from portal_inmobiliario_scanner.portal_inmobiliario_scanner_stack import PortalInmobiliarioScannerStack


app = core.App()
PortalInmobiliarioScannerStack(app, "portal-inmobiliario-scanner",env={'region': 'us-west-1'})

app.synth()
