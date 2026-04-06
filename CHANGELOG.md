# Changelog

## 0.4.1 (2026-04-06)

- added `*` support for hass collector
- added ctrl+s and back arrow for ace editor

## 0.4.0 (2026-04-06)

- added `hass` collector

## 0.4.0-rc3 (2026-04-06)

- added `now` function to template parsing and gotify and ntfy titles
- updated windy sink with new API
- fixed some possible race conditions in stateful pipelines

## 0.4.0-rc2 (2026-04-04)

- added statful cron pipelines support

## 0.4.0-rc1 (2026-04-04)

- changed to config v2 version
- added event bus
- reworked internal architecture
- added integrated config editor based on ace.js

## 0.3.1 (2026-01-25)

- rebrand the `data-collector` to `Siphon`
- move development to new github repo

## 0.3.0 (2026-01-11)

- (sinks) add the ntfy sink
    - support templated `Title` with `now` helper
    - require `url` and `topic` parameters; default priority to 3 when out of range or not set
    - send `Authorization: Bearer <token>` when `token` is set
    - POST payload to `{url}/{topic}`, accept any `2xx` response and close response body
    - use 5s HTTP client timeout
    - register **sink** via `sink.Registry` and return errors from constructor
    - added example configuration for the sink
- (sinks) added to all sinks support for new costructor with error returned, instead of just `nil`
- main: handle error return from new sink constructor

## 0.2.7 (2024-05-23)

- (dispatchers/event) trigger the event only once, when expression is met

## 0.2.6 (2024-05-08)

- (datastore) fix timeout timer reset issue

## 0.2.5 (2024-05-08)

- (dispatchers/event) add timeout trigger and IsTimeout function in sinks templates

## 0.2.4 (2024-05-02)

- ko.yaml: import embedded timezone database into application

## 0.2.3 (2024-05-02)

- (sinks/gotify) hotfix for title

## 0.2.2 (2024-05-02)

- (sinks/gotify) add template support for gotify title, example:
    add in config:

    ```yml
    ...
    sinks:
      gotify:
        params:
          title: '{{ now "15:04" }}: report from machine'
    ...
    ```

    This will add hour and minute in title of gotify message.

## 0.2.1 (2024-01-04)

- (sinks/windy) fix sink when station id is 0

## 0.2.0 (2023-11-25)

- (dispatcher/event) fix double event trigger when multiple sinks are assigned
- (collector/mqtt) subscribe after reconnect

## 0.1.3 (2023-11-24)

- (sinks/gotify) fixed priority was not being set in message from params
- (README) update info about environment variables and docker deployment

## 0.1.2 (2023-11-23)

- added 32 bit arm containers

## 0.1.1 (2023-11-23)

- added multi platform container images builds to CI config

## 0.1.0 (2023-11-23)

- first versioned release
- all basic functionality is working
- not tested very well
