/*
 * esc_pos_bluetooth
 * Created by Andrey Ushakov
 * 
 * Copyright (c) 2019-2020. All rights reserved.
 * See LICENSE for distribution and usage details.
 */

import 'dart:async';
import 'dart:io';
import 'package:esc_pos_utils/esc_pos_utils.dart';
import 'package:rxdart/rxdart.dart';
import 'package:flutter_bluetooth_basic/flutter_bluetooth_basic.dart';
import './enums.dart';

/// Bluetooth printer
class PrinterBluetooth {
  PrinterBluetooth(this._device);
  final BluetoothDevice _device;

  String get name => _device.name;
  String get address => _device.address;
  int get type => _device.type;
}

/// Printer Bluetooth Manager
class PrinterBluetoothManager {
  final BluetoothManager _bluetoothManager = BluetoothManager.instance;
  bool _isPrinting = false;
  StreamSubscription _scanResultsSubscription;
  StreamSubscription _isScanningSubscription;
  PrinterBluetooth _selectedPrinter;

  final BehaviorSubject<bool> _isScanning = BehaviorSubject.seeded(false);
  Stream<bool> get isScanningStream => _isScanning.stream;

  final BehaviorSubject<List<PrinterBluetooth>> _scanResults =
      BehaviorSubject.seeded([]);
  Stream<List<PrinterBluetooth>> get scanResults => _scanResults.stream;

  void startScan(Duration timeout) async {
    _scanResults.add(<PrinterBluetooth>[]);

    _bluetoothManager.startScan(timeout: timeout);

    _scanResultsSubscription = _bluetoothManager.scanResults.listen((devices) {
      _scanResults.add(devices.map((d) => PrinterBluetooth(d)).toList());
    });

    _isScanningSubscription =
        _bluetoothManager.isScanning.listen((isScanningCurrent) async {
      // If isScanning value changed (scan just stopped)
      if (_isScanning.value && !isScanningCurrent) {
        _scanResultsSubscription.cancel();
        _isScanningSubscription.cancel();
      }
      _isScanning.add(isScanningCurrent);
    });
  }

  void stopScan() async {
    await _bluetoothManager.stopScan();
  }

  void selectPrinter(PrinterBluetooth printer) {
    _selectedPrinter = printer;
  }

  Future<PosPrintResult> writeBytes(
    List<int> bytes, {
    int chunkSizeBytes = 20,
    int queueSleepTimeMs = 20,
  }) async {
    if (_selectedPrinter == null) {
      return Future<PosPrintResult>.value(PosPrintResult.printerNotSelected);
    } else if (_isScanning.value) {
      return Future<PosPrintResult>.value(PosPrintResult.scanInProgress);
    } else if (_isPrinting) {
      return Future<PosPrintResult>.value(PosPrintResult.printInProgress);
    }

    // We have to rescan before connecting, otherwise we can connect only once
    await _bluetoothManager.startScan(timeout: Duration(seconds: 1));
    await _bluetoothManager.stopScan();

    // Subscribe to the events
    _bluetoothManager.state.listen((state) async {
      switch (state) {
        case BluetoothManager.CONNECTED:
          print('Successfully Connected');
          break;
        case BluetoothManager.DISCONNECTED:
          print('Successfully Disconnected');
          break;
        default:
          break;
      }
    });

    // Connect
    await _bluetoothManager.connect(_selectedPrinter._device);

    if (!_isPrinting) {
      _isPrinting = true;
      final len = bytes.length;
      List<List<int>> chunks = [];
      for (var i = 0; i < len; i += chunkSizeBytes) {
        var end = (i + chunkSizeBytes < len) ? i + chunkSizeBytes : len;
        chunks.add(bytes.sublist(i, end));
      }

      for (var i = 0; i < chunks.length; i += 1) {
        await _bluetoothManager.writeData(chunks[i]);
        sleep(Duration(milliseconds: queueSleepTimeMs));
      }
    }

    _isPrinting = false;
    await _bluetoothManager.disconnect();
    return PosPrintResult.success;
  }

  Future<PosPrintResult> printTicket(
    Ticket ticket, {
    int chunkSizeBytes = 20,
    int queueSleepTimeMs = 20,
  }) async {
    if (ticket == null || ticket.bytes.isEmpty) {
      return Future<PosPrintResult>.value(PosPrintResult.ticketEmpty);
    }
    return writeBytes(
      ticket.bytes,
      chunkSizeBytes: chunkSizeBytes,
      queueSleepTimeMs: queueSleepTimeMs,
    );
  }
}
