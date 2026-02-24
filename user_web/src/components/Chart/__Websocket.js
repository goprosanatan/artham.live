import { io } from "socket.io-client";
import { apiAddress } from "@/config/runtime.js";

// Lightweight wrapper to connect and wire websocket listeners for the chart
export function connectLive({
	token,
	onConnect,
	onDisconnect,
	onAuthenticated,
	onUnauthorized,
	onBar,
	onOrderEvent,
	onDepth,
	onOptionFeature,
	onSubscribed,
	onUnsubscribed,
	onSubscribedReplay,
	onUnsubscribedReplay,
	onSubscriptions,
	onError,
} = {}) {
	const socket = io(apiAddress, {
		path: "/websocket",
		autoConnect: true,
		transports: ["websocket"],
	});

	const cleanup = () => {
		socket.off("connect", handleConnect);
		socket.off("disconnect", handleDisconnect);
		socket.off("authenticated", handleAuthenticated);
		socket.off("unauthorized", handleUnauthorized);
		socket.off("bar", handleBar);
		socket.off("order_event", handleOrderEvent);
		socket.off("depth", handleDepth);
		socket.off("option_feature", handleOptionFeature);
		socket.off("subscribed", handleSubscribed);
		socket.off("unsubscribed", handleUnsubscribed);
		socket.off("subscribed_replay", handleSubscribedReplay);
		socket.off("unsubscribed_replay", handleUnsubscribedReplay);
		socket.off("subscriptions", handleSubscriptions);
		socket.off("error", handleError);
	};

	const handleConnect = () => {
		onConnect?.();
		if (token) {
			socket.emit("authenticate", { access_token: token });
		}
	};

	const handleDisconnect = () => {
		onDisconnect?.();
	};

	const handleAuthenticated = () => {
		onAuthenticated?.();
	};

	const handleUnauthorized = (data) => {
		onUnauthorized?.(data);
	};

	const handleBar = (data) => onBar?.(data);
	const handleOrderEvent = (data) => onOrderEvent?.(data);
	const handleDepth = (data) => onDepth?.(data);
	const handleOptionFeature = (data) => onOptionFeature?.(data);
	const handleSubscribed = (data) => onSubscribed?.(data);
	const handleUnsubscribed = (data) => onUnsubscribed?.(data);
	const handleSubscribedReplay = (data) => onSubscribedReplay?.(data);
	const handleUnsubscribedReplay = (data) => onUnsubscribedReplay?.(data);
	const handleSubscriptions = (data) => onSubscriptions?.(data);
	const handleError = (err) => onError?.(err);

	socket.on("connect", handleConnect);
	socket.on("disconnect", handleDisconnect);
	socket.on("authenticated", handleAuthenticated);
	socket.on("unauthorized", handleUnauthorized);
	socket.on("bar", handleBar);
	socket.on("order_event", handleOrderEvent);
	socket.on("depth", handleDepth);
	socket.on("option_feature", handleOptionFeature);
	socket.on("subscribed", handleSubscribed);
	socket.on("unsubscribed", handleUnsubscribed);
	socket.on("subscribed_replay", handleSubscribedReplay);
	socket.on("unsubscribed_replay", handleUnsubscribedReplay);
	socket.on("subscriptions", handleSubscriptions);
	socket.on("error", handleError);

	return {
		socket,
		disconnect: () => {
			cleanup();
			socket.disconnect();
		},
		subscribe: (instruments, type) => {
			if (!Array.isArray(instruments) || instruments.length === 0) return;
			if (!type) {
				console.error("Subscription type is required (e.g., 'bars.1m', 'bars.1D')");
				return;
			}
			socket.emit("subscribe", { type, instruments });
		},
		unsubscribe: (instruments, type) => {
			if (!Array.isArray(instruments) || instruments.length === 0) return;
			if (!type) {
				console.error("Subscription type is required (e.g., 'bars.1m', 'bars.1D')");
				return;
			}
			socket.emit("unsubscribe", { type, instruments });
		},
		requestSubscriptions: (type) => {
			if (!type) {
				console.error("Subscription type is required (e.g., 'bars.1m', 'bars.1D')");
				return;
			}
			socket.emit("get_subscriptions", { type });
		},
		subscribeReplay: (sessionId, type) => {
			if (!sessionId) {
				console.error("Replay session_id is required");
				return;
			}
			if (!type) {
				console.error("Replay subscription type is required (e.g., 'bars.1m', 'bars.1D')");
				return;
			}
			socket.emit("subscribe_replay", { session_id: sessionId, type });
		},
		unsubscribeReplay: (sessionId, type) => {
			if (!sessionId) {
				console.error("Replay session_id is required");
				return;
			}
			if (!type) {
				console.error("Replay subscription type is required (e.g., 'bars.1m', 'bars.1D')");
				return;
			}
			socket.emit("unsubscribe_replay", { session_id: sessionId, type });
		},
	};
}
