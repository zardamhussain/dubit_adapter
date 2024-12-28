import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;
import 'package:daily_flutter/daily_flutter.dart';
import 'package:permission_handler/permission_handler.dart';

class DubitEvent {
  final String label;
  final dynamic value;

  DubitEvent(this.label, [this.value]);
}

enum DubitAudioDevice {
  speakerphone,
  wired,
  earpiece,
  bluetooth,
}

class Dubit {
  final String? apiKey;
  final String? apiBaseUrl;
  bool isJoined = false;
  final _streamController = StreamController<DubitEvent>();

  Stream<DubitEvent> get onEvent => _streamController.stream;

  CallClient? _client;

  Dubit([this.apiKey, this.apiBaseUrl]);

  bool isJoinedUser() {
    return isJoined;
  }

  Future<void> start({
    String webCallUrl = "",
    Duration clientCreationTimeoutDuration = const Duration(seconds: 10),
  }) async {
    if (_client != null) {
      throw Exception('Call already in progress');
    }

    print("🔄 ${DateTime.now()}: Dubit - Requesting Mic Permission...");
    var microphoneStatus = await Permission.microphone.request();
    if (microphoneStatus.isDenied) {
      microphoneStatus = await Permission.microphone.request();
      if (microphoneStatus.isPermanentlyDenied) {
        openAppSettings();
        return;
      }
    }

    var clientCreationFuture =
        _createClientWithRetries(clientCreationTimeoutDuration);

    String callUrl;

    if (webCallUrl.isNotEmpty) {
      var client = await clientCreationFuture;
      _client = client;

      callUrl = webCallUrl;
      print("🆗 ${DateTime.now()}: Dubit - Using provided Dubit Call URL");
    } else {
      if (apiKey == null || apiKey!.isEmpty)
        throw Exception("apiKey is required");

      print("🔄 ${DateTime.now()}: Dubit - Preparing Call & Client...");

      var baseUrl = apiBaseUrl ?? 'https://test-api.dubit.live';
      var url = Uri.parse('$baseUrl/meeting/new-meeting');

      var headers = {
        'Authorization': 'Bearer $apiKey',
        'Content-Type': 'application/json',
      };

      // Make the API call to get a new meeting
      var dubitCallFuture = http.get(url, headers: headers);

      // Wait for both the API call and the client creation future
      var results = await Future.wait([dubitCallFuture, clientCreationFuture]);

      var response = results[0] as http.Response;
      var client = results[1] as CallClient;

      _client = client;

      await _client!.setUsername('Faceon Event Listener');

      if (response.statusCode == 200) {
        print("🆗 ${DateTime.now()}: Dubit - Dubit Call Ready");

        var data = jsonDecode(response.body);
        callUrl = data['roomUrl'];
      } else {
        client.dispose();
        _client = null;
        print(
            '🆘 ${DateTime.now()}: Dubit - Failed to create Dubit Call. Error: ${response.body}');
        emit(DubitEvent("call-error"));
        return;
      }
    }

    print("🔄 ${DateTime.now()}: Dubit - Joining Call...");

    _client!.setUsername("Flutter");

    _client!.events.listen((event) {
      event.whenOrNull(
        activeSpeakerChanged: (participant) {
          _onAppMessage(jsonEncode({
            "type": "active-speaker",
            "meetID" : callUrl.split('/').last,
            "participant_details" : participant,
            "participant_id": participant?.id,
            "username": participant?.info.username,
          }));
        },
        callStateUpdated: (stateData) {
        switch (stateData.state) {
          case CallState.leaving:
          case CallState.left:
            _client = null;
            print("⏹️  ${DateTime.now()}: Dubit - Call Ended.");
            emit(DubitEvent("call-end"));
            break;
          case CallState.joined:
            print("🆗 ${DateTime.now()}: Dubit - Joined Call");
            break;
          default:
            break;
        }
      }, participantLeft: (participantData) {
        if (participantData.info.isLocal) {
          isJoined = false;
          _client?.leave();
          return;
        }
        _onAppMessage(jsonEncode({
          "type": "user-left",
          "participant_id": participantData.id,
          "username": participantData.info.username
        }));
      }, appMessageReceived: (messageData, id) {
        final messageWithMeetId = jsonDecode(messageData);
        messageWithMeetId['meetID'] = callUrl.split('/').last;
        _onAppMessage(jsonEncode(messageWithMeetId));
      }, participantUpdated: (participantData) {
        if (participantData.info.username == "Dubit Speaker" &&
            participantData.media?.microphone.state == MediaState.playable) {
          print("📤 ${DateTime.now()}: Dubit - Sending Ready...");
          _client?.sendAppMessage(jsonEncode({'message': "playable"}), null);
        }
      }, participantJoined: (participantData) {
        if (participantData.info.username == "Dubit Speaker" &&
            participantData.media?.microphone.state == MediaState.playable) {
          print("📤 ${DateTime.now()}: Dubit - Sending Ready...");
          _client?.sendAppMessage(jsonEncode({'message': "playable"}), null);
        }
        if (participantData.info.isLocal) {
          isJoined = true;
        }
      });
    });

    try {
      await _client!.join(
        url: Uri.parse(callUrl),
        clientSettings: const ClientSettingsUpdate.set(
          inputs: InputSettingsUpdate.set(
            microphone: MicrophoneInputSettingsUpdate.set(
                isEnabled: BoolUpdate.set(false)),
            camera:
                CameraInputSettingsUpdate.set(isEnabled: BoolUpdate.set(false)),
          ),
        ),
      );
      _client!.setIsPublishing(camera: false,microphone: false);
      const subscriptionProfile = SubscriptionProfile.base;
      const mediaSubscriptionUpdateSettings = MediaSubscriptionSettingsUpdate.set(
        camera: VideoSubscriptionSettingsUpdate.set(subscriptionState: SubscriptionStateUpdate.unsubscribed), 
        screenVideo: VideoSubscriptionSettingsUpdate.set(subscriptionState: SubscriptionStateUpdate.unsubscribed), 
        microphone: AudioSubscriptionSettingsUpdate.set(subscriptionState: SubscriptionStateUpdate.unsubscribed), 
        screenAudio: AudioSubscriptionSettingsUpdate.set(subscriptionState: SubscriptionStateUpdate.unsubscribed), 
      );
      await _client!.updateSubscriptionProfiles(forProfiles: {
        subscriptionProfile : mediaSubscriptionUpdateSettings
      });
    } catch (e) {
      print('🆘 ${DateTime.now()}: Dubit - Failed to join call: $e');
      throw Exception('Failed to join call: $e');
    }
  }

  Future<CallClient> _createClientWithRetries(
    Duration clientCreationTimeoutDuration,
  ) async {
    var retries = 0;
    const maxRetries = 5;

    Future<CallClient> attemptCreation() async {
      return CallClient.create();
    }

    Future<CallClient> createWithTimeout() async {
      var completer = Completer<CallClient>();
      Future.delayed(clientCreationTimeoutDuration).then((_) {
        if (!completer.isCompleted) {
          print("⏳ ${DateTime.now()}: Dubit - Client creation timed out.");
          completer
              .completeError(TimeoutException('Client creation timed out'));
        }
      });

      attemptCreation().then((client) {
        if (!completer.isCompleted) {
          completer.complete(client);
        }
      }).catchError((error) {
        if (!completer.isCompleted) {
          completer.completeError(error);
        }
      });

      return completer.future;
    }

    while (retries < maxRetries) {
      try {
        print(
            "🔄 ${DateTime.now()}: Dubit - Creating client (Attempt ${retries + 1})...");
        var client = await createWithTimeout();
        print("🆗 ${DateTime.now()}: Dubit - Client Created");
        return client;
      } catch (e) {
        retries++;
        if (retries >= maxRetries) {
          print(
              "🆘 ${DateTime.now()}: Dubit - Failed to create client after $maxRetries attempts.");
          rethrow;
        }
      }
    }

    // This line should theoretically never be reached due to the rethrow above
    throw Exception('Client creation failed after $maxRetries retries');
  }

  Future<void> send(dynamic message) async {
    await _client!.sendAppMessage(jsonEncode(message), null);
  }

  void _onAppMessage(String msg) {
    try {
      var parsedMessage = jsonDecode(msg);
      if (parsedMessage == "listening") {
        print("✅ ${DateTime.now()}: Dubit - Assistant Connected.");
        emit(DubitEvent("call-start"));
      }

      emit(DubitEvent("message", parsedMessage));
    } catch (parseError) {
      print("Error parsing message data: $parseError");
    }
  }

  Future<void> stop() async {
    if (_client == null) {
      throw Exception('No call in progress');
    }
    await _client!.leave();
  }

  void setMuted(bool muted) {
    _client!.updateInputs(
        inputs: InputSettingsUpdate.set(
      microphone:
          MicrophoneInputSettingsUpdate.set(isEnabled: BoolUpdate.set(!muted)),
    ));
  }

  bool isMuted() {
    return _client!.inputs.microphone.isEnabled == false;
  }

  @Deprecated(
    "Use [setDubitAudioDevice] instead. Deprecated because unusable if user does not depend of daily_flutter",
  )

  /// use [setDubitAudioDevice] instead
  void setAudioDevice({required DeviceId deviceId}) {
    _client!.setAudioDevice(deviceId: deviceId);
  }

  void setDubitAudioDevice({required DubitAudioDevice device}) {
    // _client!.setAudioDevice(
    //   deviceId: switch (device) {
    //     DubitAudioDevice.speakerphone => DeviceId.speakerPhone,
    //     DubitAudioDevice.wired => DeviceId.wired,
    //     DubitAudioDevice.earpiece => DeviceId.earpiece,
    //     DubitAudioDevice.bluetooth => DeviceId.bluetooth,
    //   },
    // );
  }

  void emit(DubitEvent event) {
    _streamController.add(event);
  }

  void dispose() {
    _streamController.close();
  }
}
