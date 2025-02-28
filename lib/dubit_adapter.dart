import 'dart:async';
import 'dart:convert';
import 'package:flutter/foundation.dart';
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
  final bool debug; // Debug variable
  bool isJoined = false;
  final _streamController = StreamController<DubitEvent>();

  final Map<String, String> _botIds = {};
  final Set<String> _isbotMuted = {};
  final Map<String, String> _fromLangCodeToBotId = {};
  String? dubitRoomUrl;

  Stream<DubitEvent> get onEvent => _streamController.stream;

  CallClient? _client;

  Dubit(
    [this.apiKey,
    this.apiBaseUrl = 'https://test-api.dubit.live',
    this.debug = false] // Default value for debug
  );

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

    _printDebug("🔄 ${DateTime.now()}: Dubit - Requesting Mic Permission...");
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
      _printDebug("🆗 ${DateTime.now()}: Dubit - Using provided Dubit Call URL");
    } else {
      if (apiKey == null || apiKey!.isEmpty) {
        throw Exception("apiKey is required");
      }

      _printDebug("🔄 ${DateTime.now()}: Dubit - Preparing Call & Client...");

      var url = Uri.parse('$apiBaseUrl/meeting/new-meeting');

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
        _printDebug("🆗 ${DateTime.now()}: Dubit - Dubit Call Ready");

        var data = jsonDecode(response.body);
        callUrl = data['roomUrl'];
      } else {
        client.dispose();
        _client = null;
        _printDebug(
            '🆘 ${DateTime.now()}: Dubit - Failed to create Dubit Call. Error: ${response.body}');
        emit(DubitEvent("call-error"));
        return;
      }
    }

    _printDebug("🔄 ${DateTime.now()}: Dubit - Joining Call...");

    _client!.setUsername("Flutter");

    _client!.events.listen((event) {
      event.whenOrNull(activeSpeakerChanged: (participant) {
        _onAppMessage(jsonEncode({
          "type": "active-speaker",
          "meetID": callUrl.split('/').last,
          "participant_details": participant,
          "participant_id": participant?.id,
          "username": participant?.info.username,
        }));
      }, callStateUpdated: (stateData) {
        switch (stateData.state) {
          case CallState.leaving:
          case CallState.left:
            _client = null;
            _printDebug("⏹️  ${DateTime.now()}: Dubit - Call Ended.");
            emit(DubitEvent("call-end"));
            break;
          case CallState.joined:
            _printDebug("🆗 ${DateTime.now()}: Dubit - Joined Call");
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
          _printDebug("📤 ${DateTime.now()}: Dubit - Sending Ready...");
          _client?.sendAppMessage(jsonEncode({'message': "playable"}), null);
        }
      }, participantJoined: (participantData) {
        if (participantData.info.username == "Dubit Speaker" &&
            participantData.media?.microphone.state == MediaState.playable) {
          _printDebug("📤 ${DateTime.now()}: Dubit - Sending Ready...");
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
      _client!.setIsPublishing(camera: false, microphone: false);
      const subscriptionProfile = SubscriptionProfile.base;
      const mediaSubscriptionUpdateSettings =
          MediaSubscriptionSettingsUpdate.set(
        camera: VideoSubscriptionSettingsUpdate.set(
            subscriptionState: SubscriptionStateUpdate.unsubscribed),
        screenVideo: VideoSubscriptionSettingsUpdate.set(
            subscriptionState: SubscriptionStateUpdate.unsubscribed),
        microphone: AudioSubscriptionSettingsUpdate.set(
            subscriptionState: SubscriptionStateUpdate.unsubscribed),
        screenAudio: AudioSubscriptionSettingsUpdate.set(
            subscriptionState: SubscriptionStateUpdate.unsubscribed),
      );
      await _client!.updateSubscriptionProfiles(
          forProfiles: {subscriptionProfile: mediaSubscriptionUpdateSettings});
    } catch (e) {
      _printDebug('🆘 ${DateTime.now()}: Dubit - Failed to join call: $e');
      throw Exception('Failed to join call: $e');
    }
  }

  Future<void> startBots({
    required String fromLang,
    required String toLang,
    String gender = "female",
    String botType = "translation",
    String webCallUrl = "",
    bool isSingle = false,
    Duration clientCreationTimeoutDuration = const Duration(seconds: 10),
  }) async {
    if (_client != null) {
      throw Exception('Call already in progress');
    }

    _printDebug("🔄 ${DateTime.now()}: Dubit - Requesting Mic Permission...");
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
    String? token;

    if (webCallUrl.isNotEmpty) {
      var client = await clientCreationFuture;
      _client = client;

      callUrl = webCallUrl;
      _printDebug("🆗 ${DateTime.now()}: Dubit - Using provided Dubit Call URL");
    } else {
      if (apiKey == null || apiKey!.isEmpty) {
        throw Exception("apiKey is required");
      }

      _printDebug("🔄 ${DateTime.now()}: Dubit - Preparing Call & Client...");

      var url = Uri.parse('$apiBaseUrl/meeting/new-meeting');

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

      if (response.statusCode == 200) {
        _printDebug("🆗 ${DateTime.now()}: Dubit - Dubit Call Ready");

        var data = jsonDecode(response.body);
        dubitRoomUrl = data['roomUrl'];
        callUrl = data['roomUrl'];
        token = data['owner_token'];
      } else {
        client.dispose();
        _client = null;
        _printDebug(
            '🆘 ${DateTime.now()}: Dubit - Failed to create Dubit Call. Error: ${response.body}');
        emit(DubitEvent("call-error"));
        return;
      }
    }

    _printDebug("🔄 ${DateTime.now()}: Dubit - Joining Call...");

    _client!.setUsername("Flutter User");

    _client!.events.listen((event) {
      event.whenOrNull(
          callStateUpdated: (stateData) {
            switch (stateData.state) {
              case CallState.leaving:
              case CallState.left:
                _client = null;
                _printDebug("⏹️  ${DateTime.now()}: Dubit - Call Ended.");
                emit(DubitEvent("call-end"));
                break;
              case CallState.joined:
                _printDebug("🆗 ${DateTime.now()}: Dubit - Joined Call");
                break;
              default:
                break;
            }
          },
          participantLeft: (participantData) async {
            _onAppMessage(jsonEncode({
              "type": "user-left",
              "participant_id": participantData.id,
              "username": participantData.info.username
            }));

            if (participantData.info.isLocal) {
              await stop();
              return;
            }
          },
          appMessageReceived: (messageData, id) {
            final messageWithMeetId = jsonDecode(messageData);

            var fromLangCode = messageWithMeetId['from_lang'];
            
            if (_fromLangCodeToBotId.containsKey(fromLangCode)) {
              _printDebug("🤖 ${DateTime.now()}: Dubit - $fromLangCode is muted\nDATA : $messageData");
              return;
            }

            messageWithMeetId['meetID'] = callUrl.split('/').last;
            _onAppMessage(jsonEncode(messageWithMeetId));
          },
          participantUpdated: (participantData) {},
          participantJoined: (participantData) {
            
            var participantName = participantData.info.username;

            if (participantName?.contains(_client?.participants.local.id.id as String) ?? false) {
              _printDebug("PARTICIPANT DATA: $participantData");
              var id = participantData.id.id;
                _botIds[id] = participantName!; 
            }

            _onAppMessage(jsonEncode({
              "type": "joined",
              "participant_id": participantData.id,
              "username": participantData.info.username
            }));
          }
      );
    });

    try {
      await _client!.join(
        url: Uri.parse(callUrl),
        clientSettings: const ClientSettingsUpdate.set(
          inputs: InputSettingsUpdate.set(
            microphone: MicrophoneInputSettingsUpdate.set(
                isEnabled: BoolUpdate.set(true)),
            camera:
                CameraInputSettingsUpdate.set(isEnabled: BoolUpdate.set(false)),
          ),
        ),
        token: token,
      );
      var locaParticipantId = _client!.participants.local.id.id;

      await saveUser(locaParticipantId);

      await addBot(
        locaParticipantId,
        fromLang,
        toLang,
        callUrl,
        gender,
      );

      if (!isSingle) {
        await Future.delayed(Duration(seconds: 1)).then((value) async {
          await addBot(
            locaParticipantId,
            toLang,
            fromLang,
            callUrl,
            gender,
          );
        },);
      }
    } catch (e) {
      _printDebug('🆘 ${DateTime.now()}: Dubit - Failed to join call: $e');
      throw Exception('Failed to join call: $e');
    }
  }
  Future<void> botLeave(String botId) async {
    final url = '$apiBaseUrl/meeting/bot/terminate?bot_id=$botId';

    final headers = {
      'Authorization': 'Bearer $apiKey',
      'Content-Type': 'application/json'
    };

    try {
      final response = await http.post(Uri.parse(url), headers: headers);
      if (response.statusCode != 200) {
        throw Exception('Request failed with status: ${response.statusCode}');
      }
    } catch (e) {
      throw Exception('Error: $e');
    }
  }

  Future<void> addBot(
    String userId,
    String fromLanguage,
    String toLanguage,
    String roomUrl,
    String gender,
  ) async {
    final headers = {
      'Content-Type': 'application/json',
    };

    final isMale = gender.toLowerCase().contains('female') ? false : true;

    final payload = jsonEncode({
      'from_language': fromLanguage,
      'to_language': toLanguage,
      'room_url': roomUrl,
      'participant_id': userId,
      'bot_type': 'translation',
      'male': isMale,
    });

    try {
      // Join bot
      final botJoinResponse = await http.post(
        Uri.parse('$apiBaseUrl/meeting/bot/join'),
        headers: headers,
        body: payload,
      );
      if (botJoinResponse.statusCode != 200 && botJoinResponse.statusCode != 201) {
        throw Exception('Error joining bot: ${botJoinResponse.body}');
      }
    } catch (e) {
      throw Exception('Failed to join the bots: $e');
    }
  }

  Future<void> saveUser(
    String userId,
  ) async {
    final headers = {
      'Content-Type': 'application/json',
    };

    final participantPayload = jsonEncode({
      'id': userId,
    });

    try {
      // Save participant
      final participantResponse = await http.post(
        Uri.parse('$apiBaseUrl/participant'),
        headers: headers,
        body: participantPayload,
      );

      if (participantResponse.statusCode != 200 &&
          participantResponse.statusCode != 201) {
        throw Exception(
            'Error saving participant: ${participantResponse.body}');
      }
    } catch (e) {
      throw Exception('Failed to save participant[$userId]: $e');
    }
  }

  List<MapEntry<String, String>> getBotIds() {
    if (_client == null) {
      throw Exception('No call in progress');
    }

    return _botIds.entries.toList();
  }

  String get getLocalUserId => (_client != null) ? _client!.participants.local.id.id : "";

  Future<void> mute(String participantId, String fromLangCode) async {
    if (_client == null) {
      _printDebug('⏳ ${DateTime.now()}: Dubit - No call in progress');
      return;
    }

    try {
      

      var p = ParticipantId(participantId);
      _printDebug(p.toString());


      _client!.updateSubscriptions(forParticipants: {
        p : const SubscriptionSettingsUpdate.set(
          media: MediaSubscriptionSettingsUpdate.set(
            microphone: AudioSubscriptionSettingsUpdate.set(
              subscriptionState: SubscriptionStateUpdate.staged
            )
          )
        )
      });


      // var x = RemoteParticipantSettingsUpdatesById.set(updates: {
      //   p: const RemoteParticipantUpdate.set(
      //     inputsEnabled: RemoteInputsEnabledUpdate.set(
      //       microphone: false
      //     )
      //   )
      // });

      // await _client!.updateRemoteParticipants(updates: x);

      if(_botIds.containsKey(participantId)) {
        _isbotMuted.add(participantId);
        _fromLangCodeToBotId[fromLangCode] = participantId;
        _printDebug("CHECKING THE BOT IDS");
      }
      

    } catch (e) {
      _printDebug('🆘 ${DateTime.now()}: Dubit - Failed to mute participant: $e');
      throw Exception('Failed to mute participant: $e');
    }
  }

  Future<void> unmute(String participantId, String fromLangCode) async {
    if (_client == null) {
      _printDebug('⏳ ${DateTime.now()}: Dubit - No call in progress');
      return;
    }

    try {
      var p = ParticipantId(participantId);

      _client!.updateSubscriptions(forParticipants: {
        p : const SubscriptionSettingsUpdate.set(
          media: MediaSubscriptionSettingsUpdate.set(
            microphone: AudioSubscriptionSettingsUpdate.set(
              subscriptionState: SubscriptionStateUpdate.subscribed
            )
          )
        )
      });

      // var x = RemoteParticipantSettingsUpdatesById.set(updates: {
      //   p: const RemoteParticipantUpdate.set(
      //     inputsEnabled: RemoteInputsEnabledUpdate.set(
      //       microphone: true
      //     )
      //   )
      // });

      // await _client!.updateRemoteParticipants(updates: x);
      
      if(_botIds.containsKey(participantId)) {
        _isbotMuted.remove(participantId);
        _fromLangCodeToBotId.remove(fromLangCode);
      }

    } catch (e) {
      _printDebug('🆘 ${DateTime.now()}: Dubit - Failed to unmute participant: $e');
      throw Exception('Failed to unmute participant: $e');
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
          _printDebug("⏳ ${DateTime.now()}: Dubit - Client creation timed out.");
          completer.completeError(TimeoutException('Client creation timed out'));
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
        _printDebug(
            "🔄 ${DateTime.now()}: Dubit - Creating client (Attempt ${retries + 1})...");
        var client = await createWithTimeout();
        _printDebug("🆗 ${DateTime.now()}: Dubit - Client Created");
        return client;
      } catch (e) {
        retries++;
        if (retries >= maxRetries) {
          _printDebug(
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
        _printDebug("✅ ${DateTime.now()}: Dubit - Assistant Connected.");
        emit(DubitEvent("call-start"));
      }

      emit(DubitEvent("message", parsedMessage));
    } catch (parseError) {
      _printDebug("Error parsing message data: $parseError");
    }
  }

  Future<void> stop() async {
    if (_client == null) {
      throw Exception('No call in progress');
    }

    for (var p in _client!.participants.remote.entries) {
      botLeave(p.key.id);
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
    _client!.setAudioDevice(
      deviceId: switch (device) {
        DubitAudioDevice.speakerphone => DeviceId.speakerPhone,
        DubitAudioDevice.wired => DeviceId.wired,
        DubitAudioDevice.earpiece => DeviceId.earpiece,
        DubitAudioDevice.bluetooth => DeviceId.bluetooth,
      },
    );
  }

  void emit(DubitEvent event) {
    _streamController.add(event);
  }

  void dispose() {
    _streamController.close();
  }

  void _printDebug(String message) {
    if(!debug) return;
    if (kDebugMode) {
      print(message);
    }
  }
}
