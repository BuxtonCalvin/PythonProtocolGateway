# Finding Out What's On Your Hardware: Create Protocol, Create Scraper, and Analyze

This guide is for anyone who wants to connect a new piece of hardware to MPG
(Multi-Protocol Gateway) when there isn't already a ready-made setup for it.
No prior experience with MPG is assumed — every term is explained the first
time it's used.

If your device is already listed in MPG's device documentation, you probably
don't need this guide — just follow the setup instructions for your specific
device. This guide is for the case where you're starting from nothing.

---

## 1. A Few Words You'll See Everywhere

You don't need to memorize this section — just come back to it if a word
below trips you up later.

| Word | What it means |
| --- | --- |
| **Device** | One physical piece of hardware — an inverter, a battery, a meter — that MPG talks to. |
| **Register** | A single memory slot inside the hardware that holds one piece of information, like a voltage or a temperature. Hardware exposes its data as a list of numbered registers. |
| **Register map** | A spreadsheet-like list that says what each register number *means* — "register 40" is "Battery Voltage," for example. Without a register map, MPG just sees a wall of numbers with no labels. |
| **Protocol** | The named register map for one manufacturer's device (or one firmware version of it). MPG ships with hundreds of these already. |
| **Modbus** | The communication standard most supported hardware uses. It's the "language" MPG and the device speak to each other in. |
| **Holding / Input / Coil / Discrete** | The four different "categories" of registers that the Modbus standard defines. A piece of hardware might use only one of these categories, or a mix. Don't worry about the technical difference — just know these are the four places data can live, and Analyze (Part 3) checks all of them for you. |
| **Scraper** | The part of MPG that *reads* data from a physical device (it "scrapes" the registers). |
| **Bridge** | The part of MPG that *sends* the data somewhere useful — to Home Assistant, a database, a dashboard, etc. A device needs a scraper to be read at all; a bridge is optional and just decides where the data goes afterward. |

---

## 2. The Big Picture

Getting a brand-new, undocumented device working in MPG usually looks like
this:

1. **Create Protocol** — Tell MPG "I have a new type of device, here's what
   to call it." This creates an empty placeholder register map you'll fill
   in later. (Skip this step if you're using a protocol MPG already ships
   with.)
2. **Create Scraper** — Tell MPG how to physically talk to your hardware
   (what port, what address, which protocol to use) and restart MPG so the
   connection goes live.
3. **Analyze** — Point MPG at the now-connected device and let it scan the
   hardware directly to discover which registers actually respond, then add
   the ones you want to your register map.
4. **Share (optional, but appreciated!)** — Once your register map works,
   consider contributing it back so the next person with your hardware
   doesn't have to start from scratch. See Part 6 below.

Once step 3 is done, your new protocol behaves exactly like any of the
built-in ones — regular scraping, bridging to Home Assistant/databases, etc.
all work normally from then on.

---

## 3. Create Protocol — Naming Your New Device Type

Use this step if your device's manufacturer isn't in MPG's list already.

1. Open the **Create Protocol** page from the MPG menu.
2. Give it a **name** — this is just a label, e.g. `acmecorp_inverter_v1`.
   Use letters, numbers, and underscores.
3. Pick a **register type** to start with — **Holding** is the most common
   choice for inverters and similar devices. Don't worry if you're not sure;
   you can add the other types later through Analyze.
4. You can optionally add a few rows by hand if you already know some
   register numbers from a manual. Most people leave this blank and let
   Analyze fill it in later (see Part 5).
5. Click **Create**.

MPG now has a "stub" protocol — a name and an empty register map, ready to
be filled in. Nothing has been read from any hardware yet.

> **Note:** This step only creates the protocol definition. It does **not**
> connect to any hardware yet — that's the next step.

---

## 4. Create Scraper — Connecting to the Hardware

This step tells MPG about one specific, physical device: where it is and
how to reach it.

1. Open the **Create Scraper** page from the MPG menu.
2. **Name your device** — letters, numbers, and underscores only.
3. **Choose a transport type** — this is *how* MPG will physically connect:
   over a network cable (TCP), a USB/serial cable (RTU), etc. Pick whatever
   matches your hardware's connection.
4. **Choose a bridge (optional)** — where you'd like the data to be sent
   once it's read (Home Assistant, a database, etc.). You can leave this
   blank for now and add it later — it has no effect on the steps that
   follow.
5. **Choose a protocol** — pick the stub protocol you made in Part 3 (or an
   existing one, if you're just adding a second copy of a known device).
6. **Choose a log level** — `INFO` is a reasonable default.
7. **Fill in connection settings** — this section changes depending on the
   transport type you picked in step 3 (IP address and port for a network
   device, serial port and baud rate for a USB/serial device, etc.).
8. You'll also see checkboxes like **Send Holding**, **Send Input**,
   **Send Coil**, **Send Discrete**. Check whichever ones you *think* apply
   to your hardware — but don't stress over getting this exactly right.
   These checkboxes only affect MPG's normal day-to-day scraping later;
   they have **no effect** on the Analyze scan in Part 5, which checks all
   four regardless of what's checked here. If you're not sure, checking
   just "Holding" is a safe default — you can adjust it afterward.
9. Click **Save**.

### Restart Required

The device you just created won't do anything yet — **MPG only picks up
new devices when it restarts.** Restart the MPG application/container now.
Once it's back up, your new device is live and ready for the next step.

---

## 5. Analyze — Discovering What's Actually On the Hardware

This is the step that does the real detective work. Analyze connects to
your now-live device and reads its registers directly, so you don't have
to guess register numbers by hand or rely on a datasheet.

### Opening Analyze

1. Go to your device's page and select it from the device drop-down.
2. If MPG can talk to the device (it's a live, connected scraper), an
   **Analyze** button will appear. If you don't see it, double check you
   restarted MPG after Create Scraper (previous section) — the button won't
   show up until the device is actually running.

### Running a Scan

1. On the Analyze page, check the box next to the protocol(s) you want to
   compare against — normally just the stub protocol you created in Part 3.
2. Click **Run Analysis**.
3. You'll see a confirmation that normal reads will pause for the
   duration of the scan (a full scan can take 30–40 minutes depending on
   your hardware and how many registers exist). Click **Proceed** to start.
4. A progress bar appears. MPG first does a quick "probe" — a handful of
   test reads spread across each of the four register categories (Holding,
   Input, Coil, Discrete) to check which ones your device actually
   responds to. This is fast, usually just a few seconds.
5. For every category that responds, MPG then does a full, thorough scan
   of that category's entire address range, reading everything the device
   is willing to answer. This is the slow part.
6. When it's done, you'll see a score for each protocol you selected, and
   a results panel showing:
   - **Add** — registers MPG found on the hardware that aren't in your
     protocol's map yet. This is how you build out your register map.
   - **Remove** — registers listed in your protocol that the hardware
     never actually responded to (only relevant if you started with some
     hand-entered rows).

### If a Category Shows "No Response" (Skipped)

Sometimes the initial probe won't find anything in one or more categories —
for example, your device might simply not support Discrete registers at
all, or (less commonly) it needed a moment longer to respond than the quick
probe allowed. When this happens:

- You'll see a warning banner explaining which categories were skipped and
  why.
- Each skipped category has a checkbox next to it. If you have reason to
  believe a category *should* have data (maybe your manual mentions Coil
  registers, for instance), check its box and click **Scan Checked
  Anyway**. This forces a full scan of that category regardless of what
  the quick probe found.
- If a category is genuinely unsupported by your hardware, leave it
  unchecked — there's nothing to find there, and skipping it saves a lot
  of time.

This is normal and expected — it doesn't mean anything is broken. It just
means MPG couldn't confirm that category on its own and is asking you to
make the final call.

### Naming the Registers You Add

Registers MPG discovers but doesn't recognize show up with a generic
placeholder name (like `register_142`). Before queuing them to add, you can
rename them to something meaningful (e.g. `Battery_SOC`) if you know what
they represent — from a manual, from watching the raw value change as you
operate the device, or by comparing against a similar device's protocol.
It's fine to leave the generic name too and rename it later; nothing about
committing is permanent or hard to undo.

### Committing Your Changes

1. For each register you want to keep, click its **Add** (or **Remove**)
   toggle to queue it. There are also "select all" checkboxes to queue an
   entire category at once.
2. Once you're happy with the queued list, click **Commit**.
3. MPG writes the changes into your protocol's register map file. If this
   is the very first commit for a brand-new stub protocol, MPG creates
   that file for you automatically — you don't need to create it by hand
   first.
4. Run **Analyze** again afterward if you'd like to confirm everything
   looks right — the newly added registers should now score as matches.

That's it — your protocol now has a real register map, built directly from
what your hardware actually reports, and it will be used the next time MPG
does its normal scheduled reads.

---

## 6. Share Your Work — Creating a Pull Request

This step is optional, but it's how MPG's library of supported devices
grows. If you just built a working register map for a device nobody else
has documented yet, sharing it back means the next person who owns that
same hardware can use it immediately instead of repeating the work you
just did.

### What a Pull Request Is

MPG's code and protocol files live on GitHub, in a public project anyone
can view. A **pull request** (often shortened to "PR") is simply a
proposal: "here are some files I've added or changed — please consider
including them in the project." The project's maintainer reviews it, can
ask questions or suggest tweaks, and then merges it in if it looks good.
You don't need permission to open one, and it doesn't change anything for
anyone else until a maintainer accepts it — so there's no risk in trying.

### The Easy Way — Editing Directly on GitHub

If all you changed is your new protocol's `.json` file and its
`.holding_registry_map.csv` (or whichever register-type CSVs you filled
in), you don't need to install any extra software. GitHub's website can
do the whole thing for you:

1. Find the files on your own computer — they're inside MPG's `protocols/`
   folder, under the manufacturer name you chose in Create Protocol.
2. Go to the project's page:
   <https://github.com/BuxtonCalvin/MultiProtocolGateway>
3. Browse to the matching folder under `protocols/` in your browser (create
   a new folder with your manufacturer's name if one doesn't already
   exist — GitHub lets you type a folder path when uploading).
4. Click **Add file → Upload files**, and drag in your `.json` and `.csv`
   files.
5. Underneath the upload box, GitHub asks you to describe your change and
   offers a choice: commit directly to the main branch, or "Create a new
   branch for this commit and start a pull request." Choose the second
   option — this is what actually opens the pull request.
6. Click **Propose changes**, then **Create pull request** on the next
   screen. Add a short description — the device manufacturer and model is
   plenty (e.g. "Add register map for AcmeCorp Inverter v1").
7. Click **Create pull request**. Done — a maintainer will review it from
   here, and may leave comments if they'd like something adjusted.

### The Git Way — If You're Already Comfortable With It

If you're familiar with `git` and would rather work from the command line:

```bash
git clone https://github.com/BuxtonCalvin/MultiProtocolGateway.git
cd MultiProtocolGateway
git checkout -b add-acmecorp-protocol
# copy your new/updated files into protocols/<manufacturer>/
git add protocols/acmecorp
git commit -m "Add register map for AcmeCorp Inverter v1"
git push origin add-acmecorp-protocol
```

Then open the URL GitHub prints after the `push` — it takes you straight
to the "Create pull request" page. (If you don't have push access to the
main repository, fork it first from the GitHub page, and push to your fork
instead — GitHub will offer to do this automatically the first time you
try to push.)

### After You Open It

- You'll get a comment or notification if the maintainer has questions or
  requests — that's normal, not a rejection.
- You can keep updating your pull request after opening it — for example
  if you run Analyze again and find a few more registers, just repeat the
  upload/commit steps against the same branch.
- There's nothing to undo if you change your mind — you can close a pull
  request at any time with no consequences.

---

## 7. Full Walkthrough (Start to Finish)

Putting it all together, here's what a complete first-time setup looks
like for a totally undocumented device:

1. **Create Protocol** → name it `mydevice_v1`, register type `Holding`,
   leave the row list empty.
2. **Create Scraper** → name it `mydevice`, pick the matching transport
   type and connection settings, protocol `mydevice_v1`, check whichever
   Send boxes seem plausible (a guess is fine).
3. **Restart MPG.**
4. Go to the device page, select `mydevice`, click **Analyze**.
5. Check `mydevice_v1`, click **Run Analysis**, confirm the prompt, and
   wait for the scan to finish.
6. If any categories were skipped and you think they shouldn't have been,
   check them and click **Scan Checked Anyway**.
7. Review the **Add** list, rename anything you recognize, queue the rows
   you want.
8. Click **Commit**.
9. Done — `mydevice_v1` is now a real, working protocol.
10. (Optional) Upload `mydevice_v1.json` and its register-map CSV(s) to
    GitHub and open a pull request — see Part 6 above — so the next person
    with the same hardware gets it for free.

---

## 8. Frequently Asked Questions

**The Analyze button doesn't appear on my device's page.**
Make sure you restarted MPG after creating the device — new devices don't
become active until then. Also note Analyze is only available for
Modbus-based devices (network or serial Modbus); it isn't available for
other connection types like CAN bus.

**Every category came back skipped — nothing was found at all.**
This usually points to a connection problem rather than a hardware
limitation — double-check the IP address/port or serial port and baud
rate you entered in Create Scraper, and confirm the device is powered on
and wired correctly. If you're on a shared serial bus with other devices,
also confirm the device's address (slave ID) is correct.

**Can I run Analyze more than once?**
Yes, any time. It's a read-only scan — it never changes anything on its
own. Nothing is written to your protocol's files until you explicitly
click Commit.

**Will Analyze interfere with my other devices?**
It only pauses normal reads/writes for the one device being analyzed.
Other devices keep running normally.

**I checked the wrong "Send Holding/Input/Coil/Discrete" boxes when I
created the device — do I need to fix that before running Analyze?**
No. Those boxes only affect MPG's regular scheduled reads later on; the
Analyze scan checks all four categories on its own regardless of what's
checked. You can go back and correct those boxes on the device's settings
page at any time, including after you've finished with Analyze.

**Do I need to know how to code to share my register map?**
No. A register map is just a JSON file and one or more CSV files — no
programming involved. The "Easy Way" in Part 6 uses GitHub's website to
upload files and open a pull request; you never need to install anything
or touch a command line if you don't want to.

---

## See Also

- [README](../../README.md) — project overview and quick start
- [MPG on GitHub](https://github.com/BuxtonCalvin/MultiProtocolGateway) — source code, issues, and where to open a pull request
- [Live Protocol Analysis Workflow diagram](../architecture/mermaid-diagrams.md#12-flowchart--live-protocol-analysis-workflow) — the technical, step-by-step breakdown of everything this guide describes in plain language
- [Device Creation Wizard diagram](../architecture/mermaid-diagrams.md#17-flowchart--device-creation-wizard) — what happens on disk when you save a new device
- [Protocol Map Directory Structure diagram](../architecture/mermaid-diagrams.md#16-mindmap--protocol-map-directory-structure) — where the JSON descriptor and register-map CSVs end up
- [Protocols](protocols.md) — register map editing reference
