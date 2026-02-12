import { io } from "socket.io-client";

// Lightweight wrapper to connect and wire websocket listeners for the chart
export function connectLive({
	token,
	onConnect,
	onDisconnect,
	onAuthenticated,
	onUnauthorized,
	onBar,
	onOrderEvent,
	onSubscribed,
	onUnsubscribed,
	onSubscriptions,
	onError,
} = {}) {
	const socket = io(`${env.API_ADDRESS}`, {
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
		socket.off("subscribed", handleSubscribed);
		socket.off("unsubscribed", handleUnsubscribed);
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
	const handleSubscribed = (data) => onSubscribed?.(data);
	const handleUnsubscribed = (data) => onUnsubscribed?.(data);
	const handleSubscriptions = (data) => onSubscriptions?.(data);
	const handleError = (err) => onError?.(err);

	socket.on("connect", handleConnect);
	socket.on("disconnect", handleDisconnect);
	socket.on("authenticated", handleAuthenticated);
	socket.on("unauthorized", handleUnauthorized);
	socket.on("bar", handleBar);
	socket.on("order_event", handleOrderEvent);
	socket.on("subscribed", handleSubscribed);
	socket.on("unsubscribed", handleUnsubscribed);
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
	};
}
