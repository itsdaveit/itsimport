# -*- coding: utf-8 -*-
# Copyright (c) 2017, itsdave GmbH and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.database import Database
from time import sleep

class CAOFaktura(Document):
    def runCaoImport(self):
        frappe.msgprint("Import aus Datenbank " + self.cao_datenbank)
        caodb = frappe.database.Database(host=self.cao_datenbankserver, user=self.cao_user, password = self.cao_passwort)
        caodb.begin()
        caodb.use(self.cao_datenbank)
        caoAddresses = caodb.sql("Select ANREDE, KUNNUM1, NAME1, NAME2, NAME3, STRASSE, PLZ, ORT from adressen where KUNNUM1 <>'';", as_dict=1)
        erpnextCustomers = frappe.get_all('Customer')
        count_total = 0
        count_uptodate = 0
        count_save = 0
        count_insert = 0
        for caoAddress in caoAddresses:
            currentCustomerName = 'CUST-' + caoAddress['KUNNUM1']
            print("processing " + currentCustomerName)
            erpnextCustomer = frappe.get_value('Customer', {"name": currentCustomerName})
            #Wenn es bereits einen Kunden mit der Kundenummer gibt, laden wir den Datensatz
            if erpnextCustomer:
                customerDoc = frappe.get_doc('Customer',currentCustomerName)
                #Wenn es bereits einen Kunden mit der Kundenummer gibt, vergleichen wir die Felder
                changeDetected = False
                if not customerDoc.customer_name == self.assembleCustomerName(caoAddress):
                    changeDetected = True
                    print(type(customerDoc.customer_name))
                    print(type(self.assembleCustomerName(caoAddress)))
                    customerDoc.customer_name = self.assembleCustomerName(caoAddress)
                    frappe.msgprint(customerDoc.customer_name)
                    frappe.msgprint(self.assembleCustomerName(caoAddress))
                if changeDetected:
                    print("updating " + currentCustomerName)
                    customerDoc.save()
                    count_save += 1
                else:
                    print("allready up to date " + currentCustomerName)
                    count_uptodate += 1

            #Ansonsten bereiten wir einen neuen Datensatz vor
            else:
                customerDoc = frappe.get_doc({"doctype":"Customer",
                                              "naming_series":currentCustomerName,
                                              "customer_type":"Company",
                                              "do_not_autoname":True,
                                              "territory":"Germany",
                                              "customer_group":"kommerziell",
                                              "customer_name":self.assembleCustomerName(caoAddress)})
                print("inserting " + currentCustomerName)
                customerDoc.insert()
                count_insert += 1
            count_total += 1



        frappe.msgprint("Es wurden " + str(count_total) + " CAO Adresen verarbeitet.")
        if count_insert > 0:
            frappe.msgprint(str(count_insert) + " Adressen eingefügt.")
        if count_save > 0:
            frappe.msgprint(str(count_save) + " Adressen aktualisiert.")
        if count_uptodate > 0:
            frappe.msgprint(str(count_uptodate) + " Adressen waren bereits aktuell.")


    def assembleCustomerName(self, caoAddress):
        #Namen zusammensetzen, falls Namensfelder leer, CAO Kunde und Kundennummer verwenden
        customer_name_new = None
        if caoAddress['NAME1'] is not None:
            customer_name_new = caoAddress['NAME1'].strip()
        if caoAddress['NAME2'] is not None:
            if customer_name_new:
                customer_name_new = customer_name_new + ' ' + caoAddress['NAME2'].strip()
        if caoAddress['NAME3'] is not None:
            if customer_name_new:
                customer_name_new = customer_name_new + ' ' + caoAddress['NAME3'].strip()
        if not customer_name_new:
            customer_name_new = 'CAO Kunde ' + caoAddress['KUNNUM1'].strip()
        return customer_name_new
