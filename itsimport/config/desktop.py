# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from frappe import _

def get_data():
	return [
		{
			"module_name": "itsimport",
			"color": "red",
			"icon": "octicon octicon-clippy",
			"type": "module",
			"label": _("itsimport")
		}
	]
