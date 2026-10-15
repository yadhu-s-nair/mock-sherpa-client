"""Map model for realistic mock sherpas: follow the lane graph and find the gates (exclusion zones) a route crosses.

Reads what a map zip already contains: graph_object.json (nodes_info / edges_info, directed) or the v5 graph_object_v5.json
(edges name their nodes: start_node / end_node; stations in stations_info) and ez.json (ez_gates:
polygon_edges, apply_dist, transit_visa_release_dist, ...). Stdlib only (runs on cpval's Python 3.8).

How it relates to the real system (read from fleet_manager and mule, not guessed):
  * FM never links a trip to zones: the sherpa decides from its own geometry (mule ez_policy.py / visa_utils.py). A visa
    is requested when the sherpa is inside the gate polygon inflated by `apply_dist`, and released once it is outside
    the polygon inflated by `transit_visa_release_dist`. Who gets the gate is FM's decision (handlers.py visa logic).
  * The real route comes from mule's dynamic router (lane constraints, orientation, turning); this is a shortest path
    over the same graph. The corridor a route takes through a gate is dictated by the graph, so which gates are crossed
    - and therefore who contends for them - matches closely, but it is an approximation, not the router's exact path.
"""
import heapq
import json
import math
import os
import zipfile
from collections import defaultdict

STEP = 0.25          # metres between points of the dense path


def _key(p):
    return (round(float(p[0]), 3), round(float(p[1]), 3))


def _sample_hermite(p1, u1, p2, u2, step=0.3):
    """Sample points along a cubic Hermite spline curve from p1 with direction u1 to p2 with direction u2."""
    chord = math.hypot(p2[0] - p1[0], p2[1] - p1[1])
    m1 = (u1[0] * chord, u1[1] * chord)
    m2 = (u2[0] * chord, u2[1] * chord)
    n = max(3, int(chord / step))
    pts = []
    for i in range(n + 1):
        t = i / float(n)
        h00 = 2 * t**3 - 3 * t**2 + 1
        h10 = t**3 - 2 * t**2 + t
        h01 = -2 * t**3 + 3 * t**2
        h11 = t**3 - t**2
        x = h00 * p1[0] + h10 * m1[0] + h01 * p2[0] + h11 * m2[0]
        y = h00 * p1[1] + h10 * m1[1] + h01 * p2[1] + h11 * m2[1]
        pts.append((x, y))
    return pts


def _find_and_load_json(archive_or_dir, target_name):
    """Find and parse target_name JSON from a zip file or directory (supports nested paths)."""
    if not archive_or_dir:
        return None
    if os.path.isdir(archive_or_dir):
        for root, _, files in os.walk(archive_or_dir):
            if target_name in files:
                try:
                    with open(os.path.join(root, target_name), "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception:
                    return None
        return None
    elif os.path.isfile(archive_or_dir):
        try:
            with zipfile.ZipFile(archive_or_dir) as z:
                names = z.namelist()
                match = None
                for n in names:
                    if n == target_name:
                        match = n
                        break
                    elif n.endswith("/" + target_name):
                        match = n
                if match is not None:
                    return json.loads(z.read(match).decode("utf-8"))
        except Exception:
            return None
    return None


class Gate(object):
    def __init__(self, gid, d):
        self.id = gid
        self.name = d["name"]
        self.apply_dist = float(d.get("apply_dist") or 1.5)               # mule default 1.5 m
        self.release_dist = float(d.get("transit_visa_release_dist") or 0.0)
        self.tags = d.get("gate_tags") or []
        pts = [pt for edge in (d.get("polygon_edges") or []) for pt in edge] or list(d.get("line_segment") or [])
        xs, ys = [p[0] for p in pts], [p[1] for p in pts]
        self.box = (min(xs), min(ys), max(xs), max(ys)) if pts else None

    def inside(self, x, y, margin=0.0):
        b = self.box
        return b is not None and b[0] - margin <= x <= b[2] + margin and b[1] - margin <= y <= b[3] + margin


class ZoneSpan(object):
    """A gate crossed by a dense path: request at `enter`, release after `exit` (indices into the dense path)."""

    def __init__(self, gate, enter, exit_):
        self.gate, self.enter, self.exit = gate, enter, exit_


class MapModel(object):
    def __init__(self, graph_object, ez=None):
        ez = ez or {}
        self.nodes = {}
        for v in graph_object["nodes_info"].values():
            self.nodes[_key(v["pos"])] = v
        self.adj = defaultdict(list)
        by_id = {nid: _key(v["pos"]) for nid, v in graph_object["nodes_info"].items()}          # v5 edges refer to nodes by id
        for e in graph_object["edges_info"].values():
            a = _key(e["node_1"]) if "node_1" in e else by_id[e["start_node"]]
            b = _key(e["node_2"]) if "node_2" in e else by_id[e["end_node"]]
            self.adj[a].append((b, float(e["weight"])))
        self._keys = list(self.nodes)
        self.stations = {}
        for k, v in self.nodes.items():
            so = v.get("station_obj")
            if so:
                self.stations[so["station_name"]] = k
        self.anchors = []                                  # [(pose, node key)]: where a station really attaches to the graph
        for name, st in (graph_object.get("stations_info") or {}).items():                       # v5: stations sit beside the graph, not in it
            if isinstance(st, dict) and st.get("pose") and self._keys:
                joined = [by_id[n] for n in (st.get("associated_nodes") or {}) if n in by_id]       # the node FM routes the station through; the nearest node can be on another lane
                node = joined[0] if joined else self.nearest(st["pose"])
                self.stations.setdefault(st.get("station_name") or st.get("name") or name, node)
                self.anchors.append((st["pose"], node))
        self.gates = [Gate(gid, g) for gid, g in ((ez or {}).get("ez_gates") or {}).items()]
        self._keys = list(self.nodes)

    @classmethod
    def from_waypoints(cls, waypoints, webui_map=None, ez=None):
        inst = cls.__new__(cls)
        inst._init_from_waypoints(waypoints, webui_map or {}, ez or {})
        return inst

    def _init_from_waypoints(self, waypoints, webui_map, ez):
        self.nodes = {}
        self.adj = defaultdict(list)
        self.gates = [Gate(gid, g) for gid, g in ((ez or {}).get("ez_gates") or {}).items()]

        for r in waypoints.get("routes", []):
            if not isinstance(r, list) or len(r) < 2:
                continue
            pair, legs = r[0], r[1]
            if not isinstance(legs, list):
                continue
            for leg in legs:
                if not isinstance(leg, list) or len(leg) < 2:
                    continue
                mode, segs = leg[0], leg[1]
                if mode not in ("forward", "reverse") or not isinstance(segs, list):
                    continue
                for seg in segs:
                    if not isinstance(seg, list) or not seg:
                        continue
                    if seg[0] == "straight" and len(seg) >= 3:
                        p1, p2 = seg[1], seg[2]
                        chord = math.hypot(p2[0] - p1[0], p2[1] - p1[1])
                        n = max(1, int(chord / 1.0))
                        line_pts = [
                            (p1[0] + (p2[0] - p1[0]) * i / n, p1[1] + (p2[1] - p1[1]) * i / n)
                            for i in range(n + 1)
                        ]
                    elif seg[0] == "turn" and len(seg) >= 5:
                        line_pts = _sample_hermite(seg[1], seg[2], seg[3], seg[4], step=0.3)
                    else:
                        continue

                    for a, b in zip(line_pts, line_pts[1:]):
                        ka, kb = _key(a), _key(b)
                        if ka != kb:
                            self.nodes[ka] = {"pos": list(ka)}
                            self.nodes[kb] = {"pos": list(kb)}
                            w = math.hypot(kb[0] - ka[0], kb[1] - ka[1])
                            if not any(v == kb for v, _ in self.adj[ka]):
                                self.adj[ka].append((kb, w))

        self._keys = list(self.nodes)
        self.stations = {}
        self.anchors = []

        st_data = webui_map.get("stations", {})
        stations_list = st_data.values() if isinstance(st_data, dict) else (st_data or [])
        for s in stations_list:
            if isinstance(s, dict) and (s.get("name") or s.get("id")):
                name = s.get("name") or str(s.get("id"))
                x, y = float(s.get("x", 0.0)), float(s.get("y", 0.0))
                theta = s.get("theta")
                if theta is None and "rotation" in s:
                    theta = math.radians(float(s["rotation"]))
                pose = [x, y, float(theta or 0.0)]
                node = self.nearest(pose) if self._keys else _key(pose)
                self.stations[name] = node
                self.anchors.append((pose, node))

        if not self.stations:
            editor_stations = waypoints.get("map_editor", {}).get("stations", {})
            for sid, s in (editor_stations.items() if isinstance(editor_stations, dict) else []):
                name = s.get("name") or str(sid)
                x, y = float(s.get("x", 0.0)), float(s.get("y", 0.0))
                rot = s.get("rotation", 0.0)
                pose = [x, y, math.radians(float(rot))]
                node = self.nearest(pose) if self._keys else _key(pose)
                self.stations[name] = node
                self.anchors.append((pose, node))

        self._keys = list(self.nodes)

    @classmethod
    def from_zip(cls, path):
        # 1. Look for v5 or v4 graph_object
        g = _find_and_load_json(path, "graph_object.json") or _find_and_load_json(path, "graph_object_v5.json")
        ez = _find_and_load_json(path, "ez.json") or {}
        if g is not None:
            return cls(g, ez)

        # 2. Look for v2 waypoints.json
        wp = _find_and_load_json(path, "waypoints.json")
        if wp is not None:
            webui = _find_and_load_json(path, "webui_map.json") or {}
            return cls.from_waypoints(wp, webui, ez)

        raise KeyError(
            f"No graph_object.json, graph_object_v5.json, or waypoints.json found in {path}"
        )

    def nearest(self, pose):
        x, y = pose[0], pose[1]
        return min(self._keys, key=lambda k: (k[0] - x) ** 2 + (k[1] - y) ** 2)

    def _ends(self, pose, n=6):
        """Graph nodes a pose can attach to: the node of the station standing there, else the n nearest nodes (the nearest alone may be on a lane that leads nowhere)."""
        for p, node in self.anchors:
            if abs(p[0] - pose[0]) < 0.6 and abs(p[1] - pose[1]) < 0.6:
                return [node]
        x, y = pose[0], pose[1]
        return sorted(self._keys, key=lambda k: (k[0] - x) ** 2 + (k[1] - y) ** 2)[:n]

    def _route(self, s, goals):
        """Dijkstra from s to the cheapest of goals: (cost, node list) or None."""
        dist, prev, pq = {s: 0.0}, {}, [(0.0, s)]
        while pq:
            d, u = heapq.heappop(pq)
            if u in goals:
                nodes = [u]
                while nodes[-1] != s:
                    nodes.append(prev[nodes[-1]])
                return d, nodes[::-1]
            if d > dist.get(u, 1e18):
                continue
            for v, w in self.adj[u]:
                nd = d + w
                if nd < dist.get(v, 1e18):
                    dist[v], prev[v] = nd, u
                    heapq.heappush(pq, (nd, v))
        return None

    def path(self, start_pose, end_pose):
        """Dense [(x, y, theta)] along the lane graph from start to end, or None if the graph has no such path."""
        goals = set(self._ends(end_pose))
        best = None
        for s in self._ends(start_pose):
            r = self._route(s, goals)
            if r:
                cost = r[0] + math.hypot(s[0] - start_pose[0], s[1] - start_pose[1]) + math.hypot(r[1][-1][0] - end_pose[0], r[1][-1][1] - end_pose[1])
                if best is None or cost < best[0]:
                    best = (cost, r[1])
        if best is None:
            return None
        nodes = best[1]
        pts = [(float(start_pose[0]), float(start_pose[1]))] + nodes + [(float(end_pose[0]), float(end_pose[1]))]
        dense = []
        for a, b in zip(pts, pts[1:]):
            seg = math.hypot(b[0] - a[0], b[1] - a[1])
            n = max(1, int(seg / STEP))
            th = math.atan2(b[1] - a[1], b[0] - a[0])
            for i in range(n):
                f = i / float(n)
                dense.append((a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f, th))
        dense.append((pts[-1][0], pts[-1][1], dense[-1][2] if dense else 0.0))
        return dense

    def zones_on(self, dense):
        """Gates this path crosses, in the order they are reached."""
        spans = []
        for g in self.gates:
            idx = [i for i, p in enumerate(dense) if g.inside(p[0], p[1], g.apply_dist)]
            if not idx:
                continue
            rel = [i for i, p in enumerate(dense) if g.inside(p[0], p[1], g.release_dist)]
            spans.append(ZoneSpan(g, idx[0], max(rel) if rel else idx[-1]))
        spans.sort(key=lambda z: z.enter)
        return spans


def map_stations(path):
    """Stations of a map zip as [{"name", "pose"}] in file order, whatever the map format: graph_object.json (current),
    webui_map.json (v2 editor) or gmaj.json. Used by the Performance page to offer a station picker for any uploaded map."""
    out = []
    for fname in ("grid_map_attributes.json", "graph_object_v5.json"):         # SIL's own source, and v5's: the real station names (webui_map.json of a v5 map only has "Station 1" labels)
        info = (_find_and_load_json(path, fname) or {}).get("stations_info")
        for k, v in (info.items() if isinstance(info, dict) else []):
            if isinstance(v, dict):
                out.append({"name": v.get("station_name") or v.get("name") or k, "pose": v.get("pose")})
        if out:
            break
    g = _find_and_load_json(path, "graph_object.json")
    if g and not out:
        for v in (g.get("nodes_info") or {}).values():
            so = v.get("station_obj") if isinstance(v, dict) else None
            if so and so.get("station_name"):
                out.append({"name": so["station_name"], "pose": so.get("pose")})
    if not out:
        w = _find_and_load_json(path, "webui_map.json")
        st = (w or {}).get("stations")
        for s in (st.values() if isinstance(st, dict) else st or []):
            if isinstance(s, dict) and s.get("name"):
                theta = s.get("theta")
                if theta is None and "rotation" in s:
                    theta = math.radians(float(s["rotation"]))
                out.append({"name": s["name"], "pose": [s.get("x"), s.get("y"), theta]})
    if not out:
        m = _find_and_load_json(path, "gmaj.json")
        for s in ((m or {}).get("stations_info") or {}).values():
            n = s.get("station_name") or s.get("name")
            if n:
                out.append({"name": n, "pose": s.get("pose")})
    seen, uniq = set(), []
    for s in out:
        if s["name"] not in seen:
            seen.add(s["name"])
            uniq.append(s)
    return uniq

