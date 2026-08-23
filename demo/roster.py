"""Relay demo roster: the 8 agents and every per-agent data table.

Pure data, no imports, no logic. Edit an agent here and every surface
(cards, detail pages, approvals, glyphs, teamwork rows) follows, because
server.py renders only what this module declares. Removing an agent is
deleting its entries here; lookups in server.py all fall back safely.
"""

# Relay's back office, organised as the three people a founder would
# otherwise have to hire: a CFO who knows where the money is, an ops manager
# who keeps orders moving, and a support manager who deals with customers
# and their banks. Every agent keys off the same order, which is why this is
# one back office and not seven tools.
#
# `today` renders as "Without Relay:" — who does this work now, and what it
# costs or why it goes undone. That gap is the product. The Dispute Responder is
# the only one switched on here; the rest are off.
#
# No personas on these cards, deliberately. A five-person business has no
# risk lead and no ops lead. One person wears every hat, and that person is
# the only reader this copy is written for.
RELAY_AGENTS = [
    dict(slug="subscription_dunning", icon="chart",
        status="roadmap", desk="calling",
        role="Subscription Dunning Agent",
        desc="Retries failed subscription payments and messages the "
             "customer before they cancel.",
        today="Failed subscription payments are not retried today.",
        replaces="manual payment follow-up"),
    # --- Your risk manager -----------------------------------------------
    # --- Your support manager --------------------------------------------
    dict(slug="dispute_defender", icon="shield",
        status="live", desk="support",
        role="Dispute Responder Agent",
        desc="Gathers the proof, writes the reply, and files it before the "
             "deadline.",
        today="Proof sits across your store, the courier and your inbox. By "
              "the time it&rsquo;s gathered, the window has shut.",
        replaces="the support executive who chases proof, &#8377;18&ndash;25k a month"),
    # --- Your telecaller --------------------------------------------------
    dict(slug="cart_rescue", icon="tasks",
        status="live", desk="calling",
        role="Abandoned Cart Recovery Agent",
        desc="Calls buyers who left without paying and sends them a payment "
             "link.",
        today="A WhatsApp blast gets a fraction back. Nobody can call 400 "
              "dropped carts a day.",
        replaces="a telecaller, &#8377;15&ndash;22k a month"),
    dict(slug="payment_rescue", icon="bolt",
        status="live", desk="calling",
        role="Failed Payment Recovery Agent",
        desc="Reads why a payment failed, waits a few minutes, then calls "
             "and sends a fresh link.",
        today="A failed UPI payment is just a lost order. Nobody reads a "
              "decline code.",
        replaces="a telecaller, &#8377;15&ndash;22k a month"),
    dict(slug="cod_guard", icon="note",
        status="roadmap", desk="calling",
        role="COD Confirmation Agent",
        desc="Confirms COD orders before dispatch and blocks addresses that "
             "keep failing.",
        today="Someone works the COD list every morning. COD is half your "
              "orders and a fifth of them come back.",
        replaces="the morning COD calling shift, &#8377;15&ndash;22k a month"),
]

# One rescue, followed through: the multithreaded story a founder needs to
# see to believe a caller agent. Voice and WhatsApp are one conversation to
# the buyer; the agent waits, switches channel, retries at a sane hour, and
# stops when told. Times are offsets, not clock times, so the story is
# stable however long the demo has been up.
AGENT_THREADS = {
    "cart_rescue": dict(
        opening="6:12 PM. Vikram leaves a &#8377;1,899 Donut Bed order at checkout.",
        closing="Won back &#8377;1,899. Three touches over two days, then it "
                "stops. A buyer who says stop is never contacted again.",
        won="&#8377;1,899 paid",
        rows=[
            ("+30 min", "call", "Calls Vikram. No answer. It does not ring "
             "twice in a row.", "missed"),
            ("+32 min", "wa", "&ldquo;Your cart is saved. Pay in one tap "
             "whenever you are ready.&rdquo; Payment link attached.", "delivered"),
            ("next day, 11 AM", "call", "Second call, at a sane hour. Vikram "
             "answers: he wanted COD. The agent offers the prepaid "
             "discount instead.", "2 min call"),
            ("+2 min", "wa", "Fresh link with the discount applied.", "paid"),
        ]),
    "payment_rescue": dict(
        opening="8:41 PM. Sneha&rsquo;s UPI payment fails on a &#8377;549 "
                "refill. The bank timed out; she does not know if she paid.",
        closing="Saved the order the same evening. The agent read the "
                "decline reason first, so every follow-up matched what "
                "actually went wrong.",
        won="&#8377;549 paid",
        rows=[
            ("+5 min", "wa", "&ldquo;That payment did not go through and "
             "nothing was charged. Here is a fresh link.&rdquo;", "read"),
            ("+20 min", "call", "No payment yet, so it calls. Sneha retries "
             "on the call; her card declines this time (limit).", "3 min call"),
            ("+1 min", "wa", "New link with UPI and card both open, plus "
             "&ldquo;pay on delivery&rdquo; as the fallback.", "paid"),
        ]),
}

# A day on the job, one per agent: the fastest way to believe an agent is
# to watch one shift. Three or four beats, real objects, real amounts, and
# the gate visible wherever money would move.
AGENT_DAYS = {
    "cod_guard": [
        ("10:00 AM", "38 COD orders lined up for dispatch today.", ""),
        ("10:20 AM", "31 confirmed on call, 4 more on WhatsApp.", "confirmed"),
        ("10:25 AM", "3 never picked up twice. Held from dispatch, pincode noted.", "held")],
    "dispute_defender": [
        ("9:14 AM", "Bank sends a dispute: &ldquo;never arrived&rdquo;, &#8377;549.", ""),
        ("9:14 AM", "Proof pulled: delivery scan, WhatsApp thread, policy as it read that day.", "done"),
        ("9:15 AM", "Reply written and put in front of you. One tap.", "your approval"),
        ("9:40 AM", "Filed with the bank, exactly once. Deadline was 6 days out.", "filed")],
}

# One outcome, three-or-four steps. The Razorpay Agent Studio grammar:
# say what you get, then how, in one glance — never a wall of rows.
AGENT_STORY = {
    "dispute_defender": dict(
        outcome="Every dispute answered before the deadline",
        steps=[("Read", "the dispute the moment the bank sends it"),
               ("Gather", "the proof from your own records"),
               ("Write", "the reply: you approve it"),
               ("File", "before the deadline, exactly once")]),
    "cart_rescue": dict(
        outcome="Dropped carts called back within the hour",
        steps=[("Spot", "a cart left at checkout"),
               ("Call", "the buyer while they still care"),
               ("Send", "the payment link that closes it")]),
    "payment_rescue": dict(
        outcome="Failed payments turned back into orders",
        steps=[("Read", "why the payment failed"),
               ("Wait", "a few minutes: then call"),
               ("Send", "a fresh link that works")]),
    "cod_guard": dict(
        outcome="COD confirmed before dispatch, returns cut",
        steps=[("Call", "every COD order before it ships"),
               ("Confirm", "the buyer actually wants it"),
               ("Block", "addresses that keep bouncing")]),
}

# The operating procedure each working agent starts with: real
# standing instructions and limits, so a settings page never opens
# empty on an agent that has been on the job since March.
AGENT_SEED = {
    "dispute_defender": dict(
        rules=["Answer every dispute at least 24 hours before the "
               "bank\u2019s deadline.",
               "Lead with the delivery proof, then the buyer\u2019s "
               "own messages.",
               "Never promise a refund inside a reply.",
               "Answer in the buyer\u2019s language."],
        guards={"ask_above": "\u20b91,000"}),
    "cart_rescue": dict(
        rules=["Hindi first, English if they switch.",
               "Offer free delivery above \u20b9999 before any "
               "discount.",
               "Never call the same buyer twice in a day; the second "
               "nudge goes on WhatsApp."],
        guards={"quiet": "11 AM to 9 PM", "tries": "2 tries"}),
    "payment_rescue": dict(
        rules=["Wait 20 minutes after a failed payment; most fix "
               "themselves.",
               "Call once, then send the fresh link on WhatsApp; SMS "
               "it if unread in an hour.",
               "Never read out the failure reason unless the buyer "
               "asks."],
        guards={"quiet": "10 AM to 7 PM", "tries": "3 tries"}),
}

# The moat, made visible: one order record that every agent reads and
# writes. AGENT_LINKS is the edge list: what each agent hands the others
# and what it borrows, all keyed off the same order.
AGENT_LINKS = {
    "dispute_defender": dict(
        gives=[("cod_guard", "which buyers dispute after delivery")],
        uses=[("cod_guard", "the confirmation call, as proof")]),
    "cart_rescue": dict(
        gives=[("payment_rescue", "buyers who got stuck mid-payment")],
        uses=[("cod_guard", "which pincodes to offer prepaid instead")]),
    "payment_rescue": dict(
        gives=[("subscription_dunning", "which decline codes mean try again")],
        uses=[("cart_rescue", "what the buyer wanted in the first place")]),
    "subscription_dunning": dict(
        gives=[("payment_rescue", "which buyers fail on the same day monthly")],
        uses=[("payment_rescue", "the decline-code playbook")]),
    "cod_guard": dict(
        gives=[("cart_rescue", "the pincode truth"),
               ("dispute_defender", "confirmation calls, kept as proof")],
        uses=[("dispute_defender", "which buyers dispute after delivery")]),
}

# What one agent learned and another now uses: the exchange itself,
# written as rows a founder can read.
TEACHINGS = [
    ("cod_guard", "Pincode 400013 bounces 2 of every 5 COD parcels",
     ["cart_rescue"], "offers those buyers prepaid with a discount instead"),
]

# ------------------------------------------------------------- agent goals
# Every agent claims ONE number it is hired to move. The goal is a sentence
# a founder would say out loud; progress is counted from actions, and every
# listed action names what it added. The Dispute Responder's number is computed
# from the real record; the rest carry the demo week's numbers, consistent
# with the morning brief.
AGENT_GOALS = {
    "dispute_defender": dict(
        goal="Win 8 of every 10 disputes", target=80,
        how="Disputes won, out of disputes answered. Whole record."),
    "cart_rescue": dict(
        goal="Recover 7 of every 10 dropped carts", target=70, now=64,
        how="Carts paid within a day of the call, out of carts called. "
            "Last 30 days.",
        actions=[
            ("Called Meera T. about her ₹4,180 cart; she paid on the "
             "fresh link", "+1 cart back"),
            ("WhatsApp follow-up to Rohan D.; paid this morning",
             "+1 cart back"),
            ("Called Sana K. twice; no answer, queued for this evening",
             "still open")]),
    "payment_rescue": dict(
        goal="Bring back 6 of every 10 failed payments", target=60, now=58,
        how="Failed payments completed after its call or message, out of "
            "failures it chased. Last 30 days.",
        actions=[
            ("Messaged the 12 buyers whose UPI timed out on Tuesday; "
             "7 paid", "+7 payments back"),
            ("Called Vikram R. about a card decline; paying by Friday",
             "promised"),
            ("3 buyers quiet after two tries; parked, not pestered",
             "still open")]),
    "cod_guard": dict(
        goal="Confirm 9 of every 10 COD orders before dispatch",
        target=90, now=82,
        how="COD orders confirmed by call or message before they ship, "
            "out of all COD orders.",
        actions=[
            ("Confirmed 31 of today's 38 COD orders before noon",
             "+31 confirmed"),
            ("Held 3 orders after two unanswered calls each",
             "pending approval"),
            ("Learned pincode 4000xx answers after 6 PM, from Cart "
             "Rescue's notes", "sharper calls")]),
}

# ---- The service tiles: one purposeful glyph per agent, colored by ----
# ---- faculty, the way a super app draws its services. Keep the     ----
# ---- faculty map in sync with FAMILIES in agents_content.          ----
AGENT_GLYPHS = {
    "cart_rescue": '<circle cx="9" cy="20" r="1.5"/><circle cx="17" cy="20" r="1.5"/><path d="M3 4h2l2.2 11.5H18l2-8H6"/>',
    "payment_rescue": '<rect x="3" y="6" width="18" height="13" rx="2.5"/><path d="M3 10.5h18M7 15h4"/>',
    "subscription_dunning": '<rect x="4" y="6" width="16" height="15" rx="2"/><path d="M4 10.5h16M8.5 3v5M15.5 3v5"/>',
    "dispute_defender": '<path d="m12 3 7 3v6c0 4.5-3 7.5-7 9-4-1.5-7-4.5-7-9V6l7-3z"/><path d="m9 12 2 2 4-4.5"/>',
    "cod_guard": '<path d="m4 8 8-4 8 4v9l-8 4-8-4V8z"/><path d="m9.5 12 2 2 3.5-3.5"/>',
    "__default__": '<path d="M12 3v5.5M12 15.5V21M3 12h5.5M15.5 12H21M6 6l3.5 3.5M14.5 14.5 18 18M18 6l-3.5 3.5M9.5 14.5 6 18"/>',
}

_FACULTY = {}

for _f, _slugs in [
    ("Sells", ("cart_rescue", "payment_rescue", "subscription_dunning")),
    ("Protects", ("dispute_defender", "cod_guard")),
]:
    for _sl in _slugs:
        _FACULTY[_sl] = _f

_TINT = {
    "Plans": ("#DDF0EE", "#0F766E"),
    "Sells": ("#E5F3E1", "#0B7A3E"),
    "Collects": ("#FBEED3", "#A9700B"),
    "Protects": ("#FBE5E5", "#C2374B"),
    "Cares": ("#ECE7FA", "#6741D9"),
    "Reports": ("#E1EDFA", "#2563EB"),
    "custom": ("#F1F0EA", "#5B5E52"),
}

# ------------------------------------------------------------- proposals
# The employee thesis, made mechanical: every acting agent ends its job in
# finished work plus ONE decision. Same card shape, same store, same
# lifecycle for all of them; a decision is written down and cannot be
# re-decided. Reporting agents close their loop in the morning note instead.
PROPS_DEF = {
    "cod_guard": dict(
        stake="&#8377;2,141", stake_n=2141,
        ifno="&#8377;2,141 shipped at a 40% bounce risk", ifyes="3 held; slots go to confirmed orders",
        kicker="Dispatch hold", rail="3 COD orders held",
        title="Hold 3 COD orders that never picked up",
        why="Two calls each, unanswered, plus a WhatsApp. Their pincode "
            "bounces 2 of every 5 COD parcels.",
        rows=[("The hold", "3 orders stay back today. Their slots go to "
               "confirmed orders."),
              ("At stake", "<b>&#8377;2,141</b> of shipping and return cost "
               "if they bounce."),
              ("Read from", "call outcomes, the pincode&rsquo;s history.")],
        yes="Hold them", no="Ship anyway",
        approved="Held from dispatch. The slots went to confirmed orders.",
        declined="Shipped as normal. The bounce risk is noted against the "
                 "pincode."),
    "cart_rescue": dict(
        stake="&#8377;31,240", stake_n=31240,
        ifno="&#8377;31,240 in carts goes cold", ifyes="12 buyers called back tonight",
        kicker="Tonight&rsquo;s calls", rail="12 carts to call",
        title="Tonight&rsquo;s rescue list: 12 dropped carts",
        why="Worth <b>&#8377;31,240</b> together. Calls between 6 and 8 PM, "
            "WhatsApp for anyone who does not pick up.",
        rows=[("The plan", "One call each, one WhatsApp fallback, then it "
               "stops. Anyone who says stop is never called again."),
              ("Read from", "dropped carts, buyer hours, past outcomes.")],
        yes="Start calling at 6", no="Skip tonight",
        approved="Calling starts at 6. Every outcome lands in your chats.",
        declined="Nobody is called tonight. The list stays."),
    "payment_rescue": dict(
        stake="&#8377;4,890", stake_n=4890,
        ifno="&#8377;4,890 stays unpaid", ifyes="Fresh links out within the hour",
        kicker="Failed payments", rail="5 payments to chase",
        title="Chase today&rsquo;s 5 failed payments",
        why="<b>&#8377;4,890</b> together, each with the decline reason "
            "already read: two bank timeouts, two limits, one wrong PIN.",
        rows=[("The plan", "WhatsApp first with a fresh link, a call only "
               "if the money matters and the link sits unused."),
              ("Read from", "decline reasons, order values, buyer hours.")],
        yes="Chase them", no="Let them go",
        approved="On it. Fresh links are out; calls follow where needed.",
        declined="Left alone. The orders stay unpaid."),
}

PROP_EXECS = {
    "cod_guard": [('3 orders held from dispatch', 'done'), ('Slots handed to confirmed orders', 'done'), ('Pincode on watch', 'running')],
    "cart_rescue": [('Call list locked: 12 buyers', 'done'), ('First calls go out at 6 PM', 'running')],
    "payment_rescue": [('Fresh links sent to all 5', 'done'), ('Two have already paid', 'done'), ('Calls follow where links sit unused', 'running')],
}
